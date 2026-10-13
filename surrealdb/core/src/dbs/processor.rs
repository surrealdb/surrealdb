use std::borrow::Cow;
use std::ops::Bound;
use std::sync::Arc;
use std::vec;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, Record, Relation};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::distinct::SyncDistinct;
use crate::dbs::{Iterable, Iterator, Operable, Options, Processable, Statement};
use crate::doc::{DocumentContext, NsDbCtx};
use crate::exec::Error as ExecError;
use crate::expr::Mock;
use crate::expr::dir::Dir;
use crate::expr::lookup::{ComputedLookupSubject, LookupKind};
use crate::idx::adjacency::{AdjacencyScope, MergedAdjacencyCursor};
use crate::idx::planner::iterators::{IndexItemRecord, IteratorRef, RecordIterator};
use crate::idx::planner::{IterationStage, RecordStrategy};
use crate::key::schema::{
	DbRoot, DecodedGraph, GraphDirPrefix, GraphForeignTablePrefix, GraphIdPrefix, RecordKey,
	RecordPrefix, ReferenceForeignFieldPrefix, ReferenceForeignTablePrefix, ReferenceIdPrefix,
	ReferenceKey,
};
use crate::key::{AnyRange, KVKeyDecode, KVValue, RawRange, Resumable, TypedRange};
use crate::kvs::lightweight::{LightweightEdgeScanner, lightweight_relation};
use crate::kvs::{DatastoreError, Direction, NORMAL_BATCH_SIZE, Transaction, Val};
use crate::val::{RecordId, RecordIdKey, RecordIdKeyRange, TableName, Value};

impl Iterable {
	#[instrument(level = "trace", name = "Iterable::iterate", skip_all)]
	pub(super) async fn iterate(
		self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
		ite: &mut Iterator,
		dis: Option<&mut SyncDistinct>,
	) -> Result<()> {
		if !self.iteration_stage_check(ctx) {
			return Ok(());
		}

		let txn = ctx.tx();
		let mut concurrent_collector = ConcurrentCollector {
			stk,
			ctx,
			opt,
			txn: &txn,
			stm,
			ite,
		};

		if let Some(dis) = dis {
			let mut distinct_collector = ConcurrentDistinctCollector {
				coll: concurrent_collector,
				dis,
			};
			distinct_collector.collect_iterable(ctx, opt, self).await?;
		} else {
			concurrent_collector.collect_iterable(ctx, opt, self).await?;
		}

		Ok(())
	}

	/// Check if the iteration stage is valid for the iterable.
	///
	/// This is only false if the iterable is a table or index and the iteration stage is building a
	/// bruteforce knn.
	fn iteration_stage_check(&self, ctx: &FrozenContext) -> bool {
		match self {
			Iterable::Table(_doc_ctx, tb, _, _) | Iterable::Index(_doc_ctx, tb, _, _) => {
				if let Some(IterationStage::BuildKnn) = ctx.get_iteration_stage()
					&& let Some(qp) = ctx.get_query_planner()
					&& let Some(exe) = qp.get_query_executor(tb)
				{
					return exe.has_bruteforce_knn();
				}
			}
			_ => {}
		}
		true
	}
}

pub(super) enum Collectable {
	Lookup(DocumentContext, LookupKind, Vec<u8>),
	RangeKey(DocumentContext, Vec<u8>),
	TableKey(DocumentContext, Vec<u8>),
	Relatable {
		doc_ctx: DocumentContext,
		f: RecordId,
		v: RelateThrough,
		w: RecordId,
		o: Option<Value>,
	},
	RecordId(DocumentContext, RecordId),
	GenerateRecordId(DocumentContext, TableName),
	Value(NsDbCtx, Value),
	Defer(DocumentContext, RecordId),
	Mergeable(DocumentContext, TableName, Option<RecordIdKey>, Value),
	KeyVal(DocumentContext, Vec<u8>, Val),
	Count(DocumentContext, usize),
	IndexItem(DocumentContext, IndexItemRecord),
	IndexItemKey(DocumentContext, IndexItemRecord),
}

impl Collectable {
	/// Processes a collected item and transforms it into a format ready for
	/// query execution.
	///
	/// This is the main entry point for the data processing pipeline. It
	/// handles different types of collected data from various sources
	/// (indexes, table scans, graph traversals, etc.) and applies the
	/// appropriate processing strategy based on the item type and execution
	/// context.
	///
	/// The `rid_only` parameter optimizes performance by skipping value
	/// fetching when only record IDs are needed (e.g., for COUNT operations or
	/// when values will be filtered out later).
	///
	/// Each variant uses a specific processing strategy optimized for its data
	/// source and use case.
	#[instrument(level = "trace", name = "Collectable::prepare", skip_all)]
	pub(super) async fn prepare(
		self,
		ctx: &FrozenContext,
		opt: &Options,
		txn: &Transaction,
		rid_only: bool,
	) -> Result<Processable> {
		match self {
			// Graph edge traversal results - requires special graph parsing and record lookup
			Self::Lookup(doc_ctx, kind, key) => {
				Self::process_lookup(doc_ctx, ctx, opt, txn, kind, key, rid_only).await
			}
			// Range scan results - lightweight processing for range queries
			Self::RangeKey(doc_ctx, key) => Self::process_range_key(doc_ctx, &key).await,
			// Table scan results - basic key-only processing for full table scans
			Self::TableKey(doc_ctx, key) => Self::process_table_key(doc_ctx, &key).await,
			// Graph relationship records - handles complex from/via/to relationship processing
			Self::Relatable {
				doc_ctx,
				f,
				v,
				w,
				o,
			} => Self::process_relatable(doc_ctx, txn, f, v, w, o, rid_only).await,
			// Direct record ID references - standard record processing
			Self::RecordId(doc_ctx, record_id) => {
				Self::process_record(opt, doc_ctx, txn, record_id, rid_only).await
			}
			// Table identifiers - used for table-level operations
			Self::GenerateRecordId(doc_ctx, table) => Self::process_yield(doc_ctx, table).await,
			// Pre-computed values - no additional processing needed
			Self::Value(doc_ctx, value) => Ok(Self::process_value(doc_ctx, value)),
			// Deferred record processing - handles lazy evaluation scenarios
			Self::Defer(doc_ctx, key) => Self::process_defer(doc_ctx, key).await,
			// Records with merge operations - applies data merging logic
			Self::Mergeable(doc_ctx, tb, id, o) => {
				Self::process_mergeable(doc_ctx, tb, id, o).await
			}
			// Raw key-value pairs from storage layer
			Self::KeyVal(doc_ctx, key, val) => Ok(Self::process_key_val(doc_ctx, &key, &val)?),
			// Count aggregation results - no record processing needed
			Self::Count(doc_ctx, c) => Ok(Self::process_count(doc_ctx, c)),
			// Index scan results with values - includes pre-fetched data
			Self::IndexItem(doc_ctx, i) => {
				Self::process_index_item(doc_ctx, txn, i, rid_only).await
			}
			// Index scan results key-only - lightweight index processing
			Self::IndexItemKey(doc_ctx, i) => Ok(Self::process_index_item_key(doc_ctx, i)),
		}
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_lookup(
		mut doc_ctx: DocumentContext,
		ctx: &FrozenContext,
		opt: &Options,
		txn: &Transaction,
		kind: LookupKind,
		key: Vec<u8>,
		rid_only: bool,
	) -> Result<Processable> {
		// Parse the data from the store
		let (ft, fk) = match kind {
			LookupKind::Graph(_) => {
				// The strict, validating decode: this path handles one key per
				// lookup (cascade deletes above all), so the per-key cost of
				// full validation is irrelevant and its corruption signal is
				// not.
				let gra = DecodedGraph::decode(&key)?;
				(gra.edge.table, gra.edge.key)
			}
			LookupKind::Reference => {
				let refe = ReferenceKey::decode_key(&key)?;
				(refe.foreign_table.into_owned(), refe.foreign_key.into_owned())
			}
		};

		// Graph/reference keys may point to a table different from the originating
		// vertex table stored in doc_ctx (e.g. edge tables during cascade delete).
		// Rebuild the context so downstream processing (events, views, lives,
		// changefeeds, field validation) uses the correct table definition.
		//
		// SECURITY: when the surrounding statement carries a VERSION clause
		// (`opt.version = Some(_)`), fetch the table definition AT that
		// version. The table's SELECT permissions are taken from the
		// resulting `StoredTableDefinition`, so reading the current-catalog
		// definition would apply the present-day permission clause to a
		// historical query — bypassing any row-level WHERE that was in
		// force when the version was captured.
		if ft.as_str() != doc_ctx.tb()?.name.as_str() {
			let tb = txn
				.get_or_add_tb(None, &doc_ctx.ns().name, &doc_ctx.db().name, &ft, opt.version)
				.await?;
			let parent = NsDbCtx {
				ns: Arc::clone(doc_ctx.ns()),
				db: Arc::clone(doc_ctx.db()),
			};
			// Carry the same read/mut shape as the incoming context. Graph
			// and reference traversals are read-only in SELECT but the same
			// dispatch reaches write paths during cascade-delete, so the
			// rebuilt context must match.
			let mutating = matches!(doc_ctx, DocumentContext::NsDbTbMutCtx(_));
			doc_ctx =
				DocumentContext::initialise(ctx, &parent, tb, &ft, opt.version, mutating).await?;
		}

		// Fetch the data from the store
		let record = if rid_only {
			Arc::new(Default::default())
		} else {
			txn.get_record(
				doc_ctx.ns().namespace_id,
				doc_ctx.db().database_id,
				&ft,
				&fk,
				opt.version,
			)
			.await?
		};
		let rid = RecordId {
			table: ft,
			key: fk,
		};
		// Parse the data from the store
		let val = Operable::Value(record);
		// Process the record
		Ok(Processable {
			doc_ctx,
			record_strategy: RecordStrategy::KeysAndValues,
			generate: None,
			rid: Some(rid.into()),
			ir: None,
			val,
		})
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_range_key(doc_ctx: DocumentContext, key: &[u8]) -> Result<Processable> {
		let key = RecordKey::decode_key(key)?;
		let val = Record::new(Value::Null);
		let rid = RecordId {
			table: key.tb.into_owned(),
			key: key.id.into_owned(),
		};
		// Create a new operable value
		let val = Operable::Value(val.into());
		// Process the record
		let pro = Processable {
			doc_ctx,
			record_strategy: RecordStrategy::KeysOnly,
			generate: None,
			rid: Some(rid.into()),
			ir: None,
			val,
		};
		Ok(pro)
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_table_key(doc_ctx: DocumentContext, key: &[u8]) -> Result<Processable> {
		let key = RecordKey::decode_key(key)?;
		let rid = RecordId {
			table: key.tb.into_owned(),
			key: key.id.into_owned(),
		};
		// Process the record
		let pro = Processable {
			doc_ctx,
			record_strategy: RecordStrategy::KeysOnly,
			generate: None,
			rid: Some(rid.into()),
			ir: None,
			val: Operable::Value(Record::new(Value::Null).into_read_only()),
		};
		Ok(pro)
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_relatable(
		doc_ctx: DocumentContext,
		txn: &Transaction,
		f: RecordId,
		through: RelateThrough,
		w: RecordId,
		o: Option<Value>,
		rid_only: bool,
	) -> Result<Processable> {
		let pro = match (rid_only, through) {
			(true, RelateThrough::Table(v)) => Processable {
				doc_ctx,
				record_strategy: RecordStrategy::KeysOnly,
				generate: Some(v),
				rid: None,
				ir: None,
				val: Operable::Value(Default::default()),
			},
			(false, RelateThrough::Table(v)) => Processable {
				doc_ctx,
				record_strategy: RecordStrategy::KeysAndValues,
				generate: Some(v),
				rid: None,
				ir: None,
				val: Operable::Relate(Default::default(), f, w, o.map(|v| v.into())),
			},
			(true, RelateThrough::RecordId(v)) => Processable {
				doc_ctx,
				record_strategy: RecordStrategy::KeysOnly,
				generate: None,
				rid: Some(v.into()),
				ir: None,
				val: Operable::Value(Default::default()),
			},
			(false, RelateThrough::RecordId(v)) if o.is_some() => Processable {
				doc_ctx,
				record_strategy: RecordStrategy::KeysAndValues,
				generate: None,
				rid: Some(v.into()),
				ir: None,
				// INSERT RELATION with an explicit edge id must be create-only so a
				// duplicate returns RecordExists unless ON DUPLICATE KEY UPDATE is set.
				val: Operable::Relate(Default::default(), f, w, o.map(|v| v.into())),
			},
			(false, RelateThrough::RecordId(v)) => {
				let val = txn
					.get_record(
						doc_ctx.ns().namespace_id,
						doc_ctx.db().database_id,
						&v.table,
						&v.key,
						None,
					)
					.await?;
				let val = Operable::Relate(val, f, w, o.map(|v| v.into()));

				Processable {
					doc_ctx,
					record_strategy: RecordStrategy::KeysAndValues,
					generate: None,
					rid: Some(v.into()),
					ir: None,
					val,
				}
			}
		};

		Ok(pro)
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_record(
		opt: &Options,
		doc_ctx: DocumentContext,
		txn: &Transaction,
		record_id: RecordId,
		rid_only: bool,
	) -> Result<Processable> {
		let val = if opt.for_update && matches!(doc_ctx, DocumentContext::NsDbTbCtx(_)) {
			// A locked read registers the record key for commit-time conflict
			// detection, so it outranks the `rid_only` value-fetch skip: the
			// registration is a side effect of reading the record, not of
			// returning it. Only read-only (SELECT) record fetches lock;
			// mutating statements register conflicts through their own writes.
			txn.get_record_for_update(
				doc_ctx.ns().namespace_id,
				doc_ctx.db().database_id,
				&record_id.table,
				&record_id.key,
			)
			.await?
		} else if rid_only {
			// if it is skippable we only need the record id
			Record::new(Value::Null).into_read_only()
		} else {
			txn.get_record(
				doc_ctx.ns().namespace_id,
				doc_ctx.db().database_id,
				&record_id.table,
				&record_id.key,
				opt.version,
			)
			.await?
		};
		// Parse the data from the store
		let val = Operable::Value(val);
		// Process the document record
		let pro = Processable {
			doc_ctx,
			record_strategy: RecordStrategy::KeysAndValues,
			generate: None,
			rid: Some(record_id.into()),
			ir: None,
			val,
		};
		// Everything ok
		Ok(pro)
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_yield(doc_ctx: DocumentContext, table_name: TableName) -> Result<Processable> {
		// Pass the value through
		let pro = Processable {
			doc_ctx,
			record_strategy: RecordStrategy::KeysAndValues,
			generate: Some(table_name),
			rid: None,
			ir: None,
			val: Operable::Value(Default::default()),
		};
		Ok(pro)
	}

	#[instrument(level = "trace", skip_all)]
	fn process_value(doc_ctx: NsDbCtx, v: Value) -> Processable {
		// Try to extract the id field if present and parse as RecordId
		let rid = match &v {
			Value::RecordId(rid) => Some(Arc::new(rid.clone())),
			_ => None,
		};
		Processable {
			doc_ctx: DocumentContext::NsDbCtx(doc_ctx),
			record_strategy: RecordStrategy::KeysAndValues,
			generate: None,
			rid,
			ir: None,
			val: Operable::Value(Record::new(v).into_read_only()),
		}
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_defer(doc_ctx: DocumentContext, v: RecordId) -> Result<Processable> {
		// Process the document record
		let pro = Processable {
			doc_ctx,
			record_strategy: RecordStrategy::KeysAndValues,
			generate: None,
			rid: Some(v.into()),
			ir: None,
			val: Operable::Value(Default::default()),
		};
		Ok(pro)
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_mergeable(
		doc_ctx: DocumentContext,
		tb: TableName,
		id: Option<RecordIdKey>,
		o: Value,
	) -> Result<Processable> {
		// Process the document record
		let pro = if let Some(id) = id {
			Processable {
				doc_ctx,
				record_strategy: RecordStrategy::KeysAndValues,
				generate: None,
				rid: Some(RecordId::new(tb, id).into()),
				ir: None,
				val: Operable::Insert(Default::default(), o.into()),
			}
		} else {
			Processable {
				doc_ctx,
				record_strategy: RecordStrategy::KeysOnly,
				generate: Some(tb),
				rid: None,
				ir: None,
				val: Operable::Insert(Default::default(), o.into()),
			}
		};
		// Everything ok
		Ok(pro)
	}

	#[instrument(level = "trace", skip_all)]
	fn process_key_val(doc_ctx: DocumentContext, key: &[u8], val: &[u8]) -> Result<Processable> {
		let key = RecordKey::decode_key(key)?;
		let rid = RecordId {
			table: key.tb.into_owned(),
			key: key.id.into_owned(),
		};
		let val = Record::kv_decode_value(val, rid.clone())?;
		// Create a new operable value
		let val = Operable::Value(val.into());
		// Process the record
		Ok(Processable {
			doc_ctx,
			record_strategy: RecordStrategy::KeysAndValues,
			generate: None,

			rid: Some(rid.into()),
			ir: None,
			val,
		})
	}

	#[instrument(level = "trace", skip_all)]
	fn process_count(doc_ctx: DocumentContext, count: usize) -> Processable {
		Processable {
			record_strategy: RecordStrategy::Count,
			generate: None,
			doc_ctx,
			rid: None,
			ir: None,
			val: Operable::Count(count),
		}
	}

	#[instrument(level = "trace", skip_all)]
	fn process_index_item_key(doc_ctx: DocumentContext, i: IndexItemRecord) -> Processable {
		let (t, v, ir) = i.consume();
		Processable {
			record_strategy: RecordStrategy::KeysOnly,
			generate: None,
			doc_ctx,
			rid: Some(t),
			ir: Some(Arc::new(ir)),
			val: Operable::Value(v.unwrap_or_else(|| Record::new(Value::Null).into_read_only())),
		}
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_index_item(
		doc_ctx: DocumentContext,
		txn: &Transaction,
		i: IndexItemRecord,
		rid_only: bool,
	) -> Result<Processable> {
		let (t, v, ir) = i.consume();
		let v = if let Some(v) = v {
			// The value may already be fetched by the KNN iterator to evaluate the
			// condition
			v
		} else if rid_only {
			// if it is skippable we only need the record id
			Record::new(Value::Null).into_read_only()
		} else {
			txn.get_record(
				doc_ctx.ns().namespace_id,
				doc_ctx.db().database_id,
				&t.table,
				&t.key,
				None,
			)
			.await?
		};
		let pro = Processable {
			doc_ctx,
			record_strategy: RecordStrategy::KeysAndValues,
			generate: None,
			rid: Some(t),
			ir: Some(ir.into()),
			val: Operable::Value(v),
		};
		Ok(pro)
	}
}

pub(super) struct ConcurrentCollector<'a> {
	stk: &'a mut Stk,
	ctx: &'a FrozenContext,
	opt: &'a Options,
	txn: &'a Transaction,
	stm: &'a Statement<'a>,
	ite: &'a mut Iterator,
}
impl Collector for ConcurrentCollector<'_> {
	#[instrument(level = "trace", skip_all)]
	async fn collect(&mut self, collectable: Collectable) -> Result<()> {
		// if it is skippable don't need to process the document
		if self.ite.skippable() > 0 {
			self.ite.skipped(1);
			return Ok(());
		}

		let pro = collectable.prepare(self.ctx, self.opt, self.txn, false).await?;
		self.ite.process(self.stk, self.ctx, self.opt, self.stm, pro).await?;

		Ok(())
	}

	fn iterator(&mut self) -> &mut Iterator {
		self.ite
	}
}

pub(super) struct ConcurrentDistinctCollector<'a> {
	coll: ConcurrentCollector<'a>,
	dis: &'a mut SyncDistinct,
}

impl Collector for ConcurrentDistinctCollector<'_> {
	#[instrument(level = "trace", skip_all)]
	async fn collect(&mut self, collectable: Collectable) -> Result<()> {
		let skippable = self.coll.ite.skippable() > 0;
		// If it is skippable, we just need to collect the record id (if any)
		// to ensure that distinct can be checked.
		let pro =
			collectable.prepare(self.coll.ctx, self.coll.opt, self.coll.txn, skippable).await?;
		if self.dis.check_already_processed(&pro) {
			return Ok(());
		}

		if skippable {
			self.coll.ite.skipped(1);
			return Ok(());
		}

		self.coll
			.ite
			.process(self.coll.stk, self.coll.ctx, self.coll.opt, self.coll.stm, pro)
			.await?;

		Ok(())
	}

	fn iterator(&mut self) -> &mut Iterator {
		self.coll.ite
	}
}

pub(super) trait Collector {
	async fn collect(&mut self, collected: Collectable) -> Result<()>;

	fn max_fetch_size(&mut self) -> u32 {
		if let Some(l) = self.iterator().start_limit() {
			*l
		} else {
			NORMAL_BATCH_SIZE
		}
	}

	fn iterator(&mut self) -> &mut Iterator;

	fn check_query_planner_context<'b>(
		ctx: &'b FrozenContext,
		table: &'b TableName,
	) -> Cow<'b, FrozenContext> {
		if let Some(qp) = ctx.get_query_planner()
			&& let Some(exe) = qp.get_query_executor(table)
		{
			// Optimize executor lookup:
			// - Attach the table-specific QueryExecutor to the Context once, so subsequent
			//   per-record processing doesn’t need to search the QueryPlanner’s internal map on
			//   every document.
			// - This keeps the hot path allocation-free and avoids repeated hash lookups inside
			//   tight iteration loops.
			let mut ctx = Context::new_child(ctx);
			ctx.set_query_executor(exe.clone());
			return Cow::Owned(ctx.freeze());
		}
		Cow::Borrowed(ctx)
	}

	#[instrument(level = "trace", name = "Collector::collect_iterable", skip_all)]
	async fn collect_iterable(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		iterable: Iterable,
	) -> Result<()> {
		if ctx.is_done(None).await? {
			return Ok(());
		}

		match iterable {
			Iterable::Value(doc_ctx, v) => {
				if v.is_nullish() {
					return Ok(());
				}

				return self.collect(Collectable::Value(doc_ctx, v)).await;
			}
			Iterable::GenerateRecordId(doc_ctx, v) => {
				self.collect(Collectable::GenerateRecordId(doc_ctx, v)).await?
			}
			Iterable::RecordId(doc_ctx, v) => {
				self.collect(Collectable::RecordId(doc_ctx, v)).await?
			}
			Iterable::Defer(doc_ctx, v) => self.collect(Collectable::Defer(doc_ctx, v)).await?,
			Iterable::MockDefer(doc_ctx, mock) => {
				self.collect_mock(ctx, doc_ctx, mock, true).await?
			}
			Iterable::MockRecordId(doc_ctx, mock) => {
				self.collect_mock(ctx, doc_ctx, mock, false).await?
			}
			Iterable::Lookup {
				doc_ctx,
				kind,
				from,
				what,
			} => self.collect_lookup(ctx, opt, doc_ctx, from, kind, what).await?,
			// For Table and Range iterables, the RecordStrategy determines whether we
			// collect only keys, keys+values, or just a count without materializing records.
			Iterable::Range(doc_ctx, tb, v, rs, sc) => match rs {
				RecordStrategy::Count => {
					self.collect_range_count(ctx, opt, doc_ctx, &tb, v).await?
				}
				RecordStrategy::KeysOnly => {
					self.collect_range_keys(ctx, opt, doc_ctx, &tb, v, sc).await?
				}
				RecordStrategy::KeysAndValues => {
					self.collect_range(ctx, opt, doc_ctx, &tb, v, sc).await?
				}
			},
			Iterable::Table(doc_ctx, table, rs, sc) => {
				let ctx = Self::check_query_planner_context(ctx, &table);
				match rs {
					RecordStrategy::Count => {
						self.collect_table_count(&ctx, opt, doc_ctx, &table).await?
					}
					RecordStrategy::KeysOnly => {
						self.collect_table_keys(&ctx, opt, doc_ctx, &table, sc).await?
					}
					RecordStrategy::KeysAndValues => {
						self.collect_table(&ctx, opt, doc_ctx, &table, sc).await?
					}
				}
			}
			Iterable::Index(doc_ctx, v, irf, rs) => {
				if let Some(qp) = ctx.get_query_planner()
					&& let Some(exe) = qp.get_query_executor(&v)
				{
					// Attach the table-specific QueryExecutor to the Context to avoid
					// per-record lookups in the QueryPlanner during index scans.
					// This significantly reduces overhead inside tight iterator loops.
					let mut ctx = Context::new_child(ctx);
					ctx.set_query_executor(exe.clone());
					let ctx = ctx.freeze();
					return self.collect_index_items(&ctx, doc_ctx, irf, rs).await;
				}
				self.collect_index_items(ctx, doc_ctx, irf, rs).await?
			}
			Iterable::Mergeable(doc_ctx, tb, id, o) => {
				self.collect(Collectable::Mergeable(doc_ctx, tb, id, o)).await?
			}
			Iterable::Relatable(doc_ctx, f, v, w, o) => {
				self.collect(Collectable::Relatable {
					doc_ctx,
					f,
					v,
					w,
					o,
				})
				.await?
			}
		}

		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn start_skip<R>(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		rng: R,
		sc: Direction,
	) -> Result<Option<R>>
	where
		R: AnyRange + Resumable + Clone,
	{
		// Fast-forward a key range by skipping the first N keys when a START clause is
		// active.
		//
		// This method avoids fully materializing or processing records prior to the
		// requested offset by streaming only keys from the underlying KV store. It
		// updates the iterator's internal skipped counter and returns a narrowed
		// range to resume scanning from.
		let ite = self.iterator();
		let skippable = ite.skippable();
		if skippable == 0 {
			// There is nothing to skip, we return the original range.
			return Ok(Some(rng));
		}
		// Get the transaction
		let txn = ctx.tx();
		// We only need to iterate over keys.
		let mut cursor = txn.open_keys_cursor_raw(rng.clone(), sc, 0, opt.version).await?;
		let mut skipped = 0;
		let mut last_key: Vec<u8> = vec![];
		'outer: loop {
			// Cap the remaining budget so we don't pull past `skippable`.
			let remaining = skippable.saturating_sub(skipped).min(NORMAL_BATCH_SIZE as usize);
			if remaining == 0 {
				break;
			}
			let batch = cursor.next_batch(remaining as u32).await?;
			if batch.is_empty() {
				break;
			}
			for key in &batch {
				if ctx.is_done(Some(skipped)).await? {
					break 'outer;
				}
				last_key.clear();
				last_key.extend_from_slice(key);
				skipped += 1;
			}
		}
		// If we don't have a last key, we're done
		if last_key.is_empty() {
			return Ok(None);
		}
		// Update the iterator about the number of skipped keys
		ite.skipped(skipped);
		// Resume the next iteration after the last key this one consumed.
		Ok(Some(rng.resume_after(&last_key, sc)))
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_table(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		doc_ctx: DocumentContext,
		table: &TableName,
		sc: Direction,
	) -> Result<()> {
		// A lightweight relation has no record range: its edges are
		// enumerated from its IN tables' adjacency instead.
		if let Some(tbdef) = ctx
			.tx()
			.get_tb(doc_ctx.ns().namespace_id, doc_ctx.db().database_id, table, None)
			.await? && let Some(rel) = lightweight_relation(&tbdef.table_type)
		{
			let rel = rel.clone();
			return self.collect_lightweight(ctx, opt, doc_ctx, table, &rel, None, sc, false).await;
		}
		let ns = doc_ctx.ns().namespace_id;
		let db = doc_ctx.db().database_id;

		// Prepare the start and end keys
		let range = RecordPrefix {
			ns,
			db,
			tb: Cow::Borrowed(table),
		}
		.range()?;

		// Optionally skip keys
		let Some(rng) = self.start_skip(ctx, opt, range, sc).await? else {
			return Ok(());
		};

		// Create a new iterable range
		let txn = ctx.tx();
		let mut cursor = txn.open_vals_cursor_raw(rng, sc, 0, opt.version).await?;
		// Loop until no more entries
		let mut count = 0;
		'outer: loop {
			let batch = cursor.next_batch(NORMAL_BATCH_SIZE).await?;
			if batch.is_empty() {
				break;
			}
			for (k, v) in batch.iter() {
				if ctx.is_done(Some(count)).await? {
					break 'outer;
				}
				self.collect(Collectable::KeyVal(doc_ctx.clone(), k.to_vec(), v.to_vec())).await?;
				count += 1;
			}
		}
		// Everything ok
		Ok(())
	}

	/// Drains a [`Mock`] target one id at a time, the same lazy-streaming
	/// shape as [`Self::collect_table`] over a live key range: `Mock`'s own
	/// iterator is already `O(1)`-state (see `expr::mock::IntoIter`), so
	/// there is nothing to pre-materialize — only the per-id `Collectable`
	/// this produces is real work.
	#[instrument(level = "trace", skip_all)]
	async fn collect_mock(
		&mut self,
		ctx: &FrozenContext,
		doc_ctx: DocumentContext,
		mock: Mock,
		deferable: bool,
	) -> Result<()> {
		for (count, rid) in mock.into_iter().enumerate() {
			if ctx.is_done(Some(count)).await? {
				break;
			}
			if deferable {
				self.collect(Collectable::Defer(doc_ctx.clone(), rid)).await?;
			} else {
				self.collect(Collectable::RecordId(doc_ctx.clone(), rid)).await?;
			}
		}
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_table_keys(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		doc_ctx: DocumentContext,
		table: &TableName,
		sc: Direction,
	) -> Result<()> {
		// A lightweight relation has no record range: its edges are
		// enumerated from its IN tables' adjacency instead.
		if let Some(tbdef) = ctx
			.tx()
			.get_tb(doc_ctx.ns().namespace_id, doc_ctx.db().database_id, table, None)
			.await? && let Some(rel) = lightweight_relation(&tbdef.table_type)
		{
			let rel = rel.clone();
			return self.collect_lightweight(ctx, opt, doc_ctx, table, &rel, None, sc, false).await;
		}
		let ns = doc_ctx.ns().namespace_id;
		let db = doc_ctx.db().database_id;

		// Prepare the start and end keys
		let range = RecordPrefix {
			ns,
			db,
			tb: Cow::Borrowed(table),
		}
		.range()?;
		// Optionally skip keys
		let rng = if let Some(rng) = self.start_skip(ctx, opt, range, sc).await? {
			// Returns the next range of keys
			rng
		} else {
			// There is nothing left to iterate
			return Ok(());
		};
		// Create a new iterable range
		let txn = ctx.tx();
		let mut cursor = txn.open_keys_cursor_raw(rng, sc, 0, opt.version).await?;
		// Loop until no more entries
		let mut count = 0;
		'outer: loop {
			let batch = cursor.next_batch(NORMAL_BATCH_SIZE).await?;
			if batch.is_empty() {
				break;
			}
			for k in batch.iter() {
				if ctx.is_done(Some(count)).await? {
					break 'outer;
				}
				self.collect(Collectable::TableKey(doc_ctx.clone(), k.to_vec())).await?;
				count += 1;
			}
		}
		// Everything ok
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_table_count(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		doc_ctx: DocumentContext,
		v: &TableName,
	) -> Result<()> {
		// A lightweight relation has no record range: its edges are
		// enumerated from its IN tables' adjacency instead.
		if let Some(tbdef) =
			ctx.tx().get_tb(doc_ctx.ns().namespace_id, doc_ctx.db().database_id, v, None).await?
			&& let Some(rel) = lightweight_relation(&tbdef.table_type)
		{
			let rel = rel.clone();
			return self
				.collect_lightweight(ctx, opt, doc_ctx, v, &rel, None, Direction::Forward, true)
				.await;
		}
		let ns = doc_ctx.ns().namespace_id;
		let db = doc_ctx.db().database_id;
		let range = RecordPrefix {
			ns,
			db,
			tb: Cow::Borrowed(v),
		}
		.range()?;
		// Create a new iterable range
		let count = ctx.tx().count(range, opt.version).await?;
		// Collect the count
		self.collect(Collectable::Count(doc_ctx, count)).await?;
		// Everything ok
		Ok(())
	}

	/// The range of records of `tb` whose id falls inside `r`.
	///
	/// A record's `id` comes from its key, so the range is untyped: callers read the
	/// bytes and decode each record against the key it was stored under.
	#[instrument(level = "trace", skip_all)]
	async fn range_prepare(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		r: RecordIdKeyRange,
	) -> Result<RawRange> {
		RecordPrefix {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
		.range_where((r.start.as_ref().map(Cow::Borrowed), r.end.as_ref().map(Cow::Borrowed)))
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_range(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		doc_ctx: DocumentContext,
		table_name: &TableName,
		r: RecordIdKeyRange,
		sc: Direction,
	) -> Result<()> {
		// A lightweight relation has no record range: its edges are
		// enumerated from its IN tables' adjacency instead.
		if let Some(tbdef) = ctx
			.tx()
			.get_tb(doc_ctx.ns().namespace_id, doc_ctx.db().database_id, table_name, None)
			.await? && let Some(rel) = lightweight_relation(&tbdef.table_type)
		{
			let rel = rel.clone();
			return self
				.collect_lightweight(ctx, opt, doc_ctx, table_name, &rel, Some(&r), sc, false)
				.await;
		}
		let ns = doc_ctx.ns().namespace_id;
		let db = doc_ctx.db().database_id;
		// Prepare
		let rng = Self::range_prepare(ns, db, table_name, r).await?;
		// Optionally skip keys
		let rng = if let Some(rng) = self.start_skip(ctx, opt, rng, sc).await? {
			// Returns the next range of keys
			rng
		} else {
			// There is nothing left to iterate
			return Ok(());
		};
		// Create a new iterable range
		let txn = ctx.tx();
		let mut cursor = txn.open_vals_cursor_raw(rng, sc, 0, None).await?;
		// Loop until no more entries
		let mut count = 0;
		'outer: loop {
			let batch = cursor.next_batch(NORMAL_BATCH_SIZE).await?;
			if batch.is_empty() {
				break;
			}
			for (k, v) in batch.iter() {
				if ctx.is_done(Some(count)).await? {
					break 'outer;
				}
				self.collect(Collectable::KeyVal(doc_ctx.clone(), k.to_vec(), v.to_vec())).await?;
				count += 1;
			}
		}
		// Everything ok
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_range_keys(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		doc_ctx: DocumentContext,
		tb: &TableName,
		r: RecordIdKeyRange,
		sc: Direction,
	) -> Result<()> {
		// A lightweight relation has no record range: its edges are
		// enumerated from its IN tables' adjacency instead.
		if let Some(tbdef) =
			ctx.tx().get_tb(doc_ctx.ns().namespace_id, doc_ctx.db().database_id, tb, None).await?
			&& let Some(rel) = lightweight_relation(&tbdef.table_type)
		{
			let rel = rel.clone();
			return self
				.collect_lightweight(ctx, opt, doc_ctx, tb, &rel, Some(&r), sc, false)
				.await;
		}
		let ns = doc_ctx.ns().namespace_id;
		let db = doc_ctx.db().database_id;

		// Get the transaction
		let txn = ctx.tx();
		// Prepare
		let rng = Self::range_prepare(ns, db, tb, r).await?;
		// Optionally skip keys
		let rng = if let Some(rng) = self.start_skip(ctx, opt, rng, sc).await? {
			// Returns the next range of keys
			rng
		} else {
			// There is nothing left to iterate
			return Ok(());
		};
		// Create a new iterable range
		let mut cursor = txn.open_keys_cursor_raw(rng, sc, 0, opt.version).await?;
		// Loop until no more entries
		let mut count = 0;
		'outer: loop {
			let batch = cursor.next_batch(NORMAL_BATCH_SIZE).await?;
			if batch.is_empty() {
				break;
			}
			for k in batch.iter() {
				if ctx.is_done(Some(count)).await? {
					break 'outer;
				}
				self.collect(Collectable::RangeKey(doc_ctx.clone(), k.to_vec())).await?;
				count += 1;
			}
		}
		// Everything ok
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_range_count(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		doc_ctx: DocumentContext,
		tb: &TableName,
		r: RecordIdKeyRange,
	) -> Result<()> {
		// A lightweight relation has no record range: its edges are
		// enumerated from its IN tables' adjacency instead.
		if let Some(tbdef) =
			ctx.tx().get_tb(doc_ctx.ns().namespace_id, doc_ctx.db().database_id, tb, None).await?
			&& let Some(rel) = lightweight_relation(&tbdef.table_type)
		{
			let rel = rel.clone();
			return self
				.collect_lightweight(
					ctx,
					opt,
					doc_ctx,
					tb,
					&rel,
					Some(&r),
					Direction::Forward,
					true,
				)
				.await;
		}
		// Get the transaction
		let txn = ctx.tx();
		// Prepare
		let range =
			Self::range_prepare(doc_ctx.ns().namespace_id, doc_ctx.db().database_id, tb, r).await?;
		// Create a new iterable range
		let count = txn.count(range, opt.version).await?;
		// Collect the count
		self.collect(Collectable::Count(doc_ctx, count)).await?;
		// Everything ok
		Ok(())
	}

	/// Collects a lightweight relation's edges by scanning its `IN` tables'
	/// adjacency (see `crate::kvs::lightweight`). The scan emits edges in
	/// canonical-id order — `[in, out]` ids sort exactly as the
	/// `(in vertex, out)` pointer keys the scan walks — streaming in either
	/// direction, so memory stays bounded by the batch size and
	/// cancellation is observed per row.
	#[allow(clippy::too_many_arguments)]
	#[instrument(level = "trace", skip_all)]
	async fn collect_lightweight(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		doc_ctx: DocumentContext,
		table: &TableName,
		rel: &Relation,
		range: Option<&RecordIdKeyRange>,
		sc: Direction,
		count_only: bool,
	) -> Result<()> {
		let ns = doc_ctx.ns().namespace_id;
		let db = doc_ctx.db().database_id;
		let txn = ctx.tx();
		// An id range filters in the storekey byte domain, which the pinned
		// array-id ordering test keeps aligned with value order.
		let encode = |bound: &std::ops::Bound<RecordIdKey>| -> Result<std::ops::Bound<Vec<u8>>> {
			Ok(match bound {
				std::ops::Bound::Included(id) => std::ops::Bound::Included(
					storekey::encode_vec(id).map_err(anyhow::Error::from_boxed)?,
				),
				std::ops::Bound::Excluded(id) => std::ops::Bound::Excluded(
					storekey::encode_vec(id).map_err(anyhow::Error::from_boxed)?,
				),
				std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
			})
		};
		let bounds = match range {
			Some(r) => Some((encode(&r.start)?, encode(&r.end)?)),
			None => None,
		};
		let in_bounds = |id: &RecordIdKey| -> Result<bool> {
			let Some((start, end)) = &bounds else {
				return Ok(true);
			};
			let bytes = storekey::encode_vec(id).map_err(anyhow::Error::from_boxed)?;
			let after_start = match start {
				std::ops::Bound::Included(b) => bytes >= *b,
				std::ops::Bound::Excluded(b) => bytes > *b,
				std::ops::Bound::Unbounded => true,
			};
			let before_end = match end {
				std::ops::Bound::Included(b) => bytes <= *b,
				std::ops::Bound::Excluded(b) => bytes < *b,
				std::ops::Bound::Unbounded => true,
			};
			Ok(after_start && before_end)
		};
		let mut scanner = LightweightEdgeScanner::new(
			&txn,
			ns,
			db,
			table,
			rel,
			sc,
			opt.version,
			Some(ctx.get_index_stores().adjacency_resolve()),
		);
		if count_only {
			let mut count = 0;
			'outer: loop {
				let batch = scanner.next_batch(NORMAL_BATCH_SIZE).await?;
				if batch.is_empty() {
					break;
				}
				for rid in batch {
					if ctx.is_done(Some(count)).await? {
						break 'outer;
					}
					if in_bounds(&rid.key)? {
						count += 1;
					}
				}
			}
			return self.collect(Collectable::Count(doc_ctx, count)).await;
		}
		// START skipping is the collector's own: each `collect` call drops a
		// row while the iterator still reports skippable rows, exactly as the
		// mock collector relies on. A second skip here would double-count.
		let mut count = 0;
		'outer: loop {
			let batch = scanner.next_batch(NORMAL_BATCH_SIZE).await?;
			if batch.is_empty() {
				break;
			}
			for rid in batch {
				if ctx.is_done(Some(count)).await? {
					break 'outer;
				}
				if !in_bounds(&rid.key)? {
					continue;
				}
				count += 1;
				self.collect(Collectable::RecordId(doc_ctx.clone(), rid)).await?;
			}
		}
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_lookup(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		doc_ctx: DocumentContext,
		from: RecordId,
		kind: LookupKind,
		what: Vec<ComputedLookupSubject>,
	) -> Result<()> {
		let ns = doc_ctx.ns().namespace_id;
		let db = doc_ctx.db().database_id;
		let prefix = DbRoot {
			ns,
			db,
		};

		// Pull out options
		let tb = &from.table;
		// Get the transaction
		let txn = ctx.tx();

		if matches!(kind, LookupKind::Reference) {
			// Fetch start and end key pairs
			let ranges = if what.is_empty() {
				vec![
					ReferenceIdPrefix {
						ns: prefix.ns,
						db: prefix.db,
						tb: Cow::Borrowed(tb),
						id: Cow::Borrowed(&from.key),
					}
					.range()?,
				]
			} else {
				what.iter()
					.map(|v| reference_lookup_range(v, ns, db, tb, &from.key))
					.collect::<Result<Vec<_>>>()?
			};
			// Loop over the reference ranges
			'keys: for rng in ranges {
				// Create a new iterable range
				let mut cursor =
					txn.open_keys_cursor(rng, Direction::Forward, 0, opt.version).await?;
				// Loop until no more entries
				let mut count = 0;
				loop {
					let batch = cursor.next_batch(NORMAL_BATCH_SIZE).await?;
					if batch.is_empty() {
						break;
					}
					for key in batch.iter() {
						if ctx.is_done(Some(count)).await? {
							break 'keys;
						}
						self.collect(Collectable::Lookup(
							doc_ctx.clone(),
							kind.clone(),
							key.to_vec(),
						))
						.await?;
						count += 1;
					}
				}
			}
			return Ok(());
		}

		// Graph lookups go through the merged adjacency reader, which spans
		// the per-edge pointer keys and — once the vertex table has folded —
		// the packed blocks. Block-sourced entries carry the pointer-key
		// bytes they had before folding, so the collectable's raw-key
		// contract is unchanged. The vertex-delete cascade runs through this
		// collector, so missing folded edges here would leak them. A
		// lightweight-edge source synthesizes its edge-side adjacency from
		// its id — gated on the edge still existing, since the id alone
		// says nothing about that.
		let adjacency = crate::idx::adjacency::vertex_adjacency(&txn, ns, db, tb).await?;
		if adjacency == crate::idx::adjacency::VertexAdjacency::Lightweight
			&& !txn.record_exists(ns, db, tb, &from.key, opt.version).await?
		{
			return Ok(());
		}
		let scopes: Vec<GraphLookupScope> = match (what.is_empty(), &kind) {
			(true, LookupKind::Graph(dir)) => match dir {
				// One scope spanning both directions, in key order.
				Dir::Both => vec![GraphLookupScope {
					dir: None,
					edge_table: None,
					fk_lower: None,
					delta_range: GraphIdPrefix {
						ns: prefix.ns,
						db: prefix.db,
						tb: Cow::Borrowed(tb),
						id: Cow::Borrowed(&from.key),
					}
					.range()?,
				}],
				x => vec![GraphLookupScope {
					dir: Some(*x),
					edge_table: None,
					fk_lower: None,
					delta_range: GraphDirPrefix {
						ns: prefix.ns,
						db: prefix.db,
						tb: Cow::Borrowed(tb),
						id: Cow::Borrowed(&from.key),
						dir: *x,
					}
					.range()?,
				}],
			},
			(false, LookupKind::Graph(Dir::Both)) => what
				.iter()
				.flat_map(|v| {
					[
						graph_lookup_scope(v, ns, db, tb, &from.key, Dir::In),
						graph_lookup_scope(v, ns, db, tb, &from.key, Dir::Out),
					]
				})
				.collect::<Result<Vec<_>>>()?,
			(false, LookupKind::Graph(dir)) => what
				.iter()
				.map(|v| graph_lookup_scope(v, ns, db, tb, &from.key, *dir))
				.collect::<Result<Vec<_>>>()?,
			(_, LookupKind::Reference) => unreachable!("handled above"),
		};
		'scopes: for scope in &scopes {
			let adjacency_scope = AdjacencyScope {
				ns,
				db,
				vertex: &from,
				dir: scope.dir,
				edge_table: scope.edge_table.as_ref(),
				fk_lower: scope.fk_lower.as_ref(),
				delta_range: scope.delta_range.clone(),
			};
			let mut cursor = match adjacency {
				crate::idx::adjacency::VertexAdjacency::Lightweight => {
					MergedAdjacencyCursor::open_lightweight(&txn, &adjacency_scope)?
				}
				crate::idx::adjacency::VertexAdjacency::Normal {
					folded,
				} => {
					MergedAdjacencyCursor::open(
						&txn,
						adjacency_scope,
						folded,
						opt.version,
						Some(ctx.get_index_stores().adjacency_resolve()),
					)
					.await?
				}
			};
			let mut count = 0;
			loop {
				// Keys-only: the collectable forwards raw key bytes and
				// `process_lookup` performs the one decode, so the cursor
				// must not decode each item a second time.
				let batch = cursor.next_batch_keys(NORMAL_BATCH_SIZE).await?;
				if batch.is_empty() {
					break;
				}
				for key in batch {
					if ctx.is_done(Some(count)).await? {
						break 'scopes;
					}
					self.collect(Collectable::Lookup(doc_ctx.clone(), kind.clone(), key)).await?;
					count += 1;
				}
			}
		}
		// Everything ok
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_index_items(
		&mut self,
		ctx: &FrozenContext,
		doc_ctx: DocumentContext,
		irf: IteratorRef,
		rs: RecordStrategy,
	) -> Result<()> {
		let Some(exe) = ctx.get_query_executor() else {
			bail!(DatastoreError::QueryNotExecuted {
				message: "No QueryExecutor has been found.".to_string(),
			})
		};

		let Some(iterator) =
			exe.new_iterator(doc_ctx.ns().namespace_id, doc_ctx.db().database_id, irf).await?
		else {
			bail!(DatastoreError::QueryNotExecuted {
				message: "No iterator has been found.".to_string(),
			})
		};

		let txn = ctx.tx();
		match rs {
			RecordStrategy::Count => {
				self.collect_index_item_count(ctx, &txn, doc_ctx, iterator).await?
			}
			RecordStrategy::KeysOnly => {
				self.collect_index_item_key(ctx, &txn, doc_ctx, iterator).await?
			}
			RecordStrategy::KeysAndValues => {
				self.collect_index_item_key_value(ctx, &txn, doc_ctx, iterator).await?
			}
		}
		// Everything ok
		return Ok(());
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_index_item_key(
		&mut self,
		ctx: &FrozenContext,
		txn: &Transaction,
		doc_ctx: DocumentContext,
		mut iterator: RecordIterator,
	) -> Result<()> {
		let fetch_size = self.max_fetch_size();
		while !ctx.is_done(None).await? {
			let records: Vec<IndexItemRecord> = iterator.next_batch(ctx, txn, fetch_size).await?;
			if records.is_empty() {
				break;
			}
			for (count, record) in records.into_iter().enumerate() {
				if ctx.is_done(Some(count)).await? {
					break;
				}
				self.collect(Collectable::IndexItemKey(doc_ctx.clone(), record)).await?;
			}
		}
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_index_item_key_value(
		&mut self,
		ctx: &FrozenContext,
		txn: &Transaction,
		doc_ctx: DocumentContext,
		mut iterator: RecordIterator,
	) -> Result<()> {
		let fetch_size = self.max_fetch_size();
		while !ctx.is_done(None).await? {
			let records: Vec<IndexItemRecord> = iterator.next_batch(ctx, txn, fetch_size).await?;
			if records.is_empty() {
				break;
			}
			for (count, record) in records.into_iter().enumerate() {
				if ctx.is_done(Some(count)).await? {
					break;
				}
				self.collect(Collectable::IndexItem(doc_ctx.clone(), record)).await?;
			}
		}
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_index_item_count(
		&mut self,
		ctx: &FrozenContext,
		txn: &Transaction,
		doc_ctx: DocumentContext,
		mut iterator: RecordIterator,
	) -> Result<()> {
		let mut total_count = 0;
		let fetch_size = self.max_fetch_size();
		while !ctx.is_done(None).await? {
			let count = iterator.next_count(ctx, txn, fetch_size).await?;
			if count == 0 {
				break;
			}
			total_count += count;
		}
		self.collect(Collectable::Count(doc_ctx, total_count)).await
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum RelateThrough {
	RecordId(RecordId),
	Table(TableName),
}

impl From<(TableName, Option<RecordIdKey>)> for RelateThrough {
	fn from((table, id): (TableName, Option<RecordIdKey>)) -> Self {
		if let Some(id) = id {
			RelateThrough::RecordId(RecordId::new(table, id))
		} else {
			RelateThrough::Table(table)
		}
	}
}

impl TryFrom<Value> for RelateThrough {
	type Error = anyhow::Error;
	fn try_from(value: Value) -> Result<Self> {
		match value {
			Value::RecordId(id) => Ok(RelateThrough::RecordId(id)),
			Value::Table(table) => Ok(RelateThrough::Table(table)),
			_ => bail!(ExecError::RelateStatementOut {
				value: value.to_sql()
			}),
		}
	}
}

impl From<RelateThrough> for Value {
	fn from(v: RelateThrough) -> Self {
		match v {
			RelateThrough::RecordId(id) => Value::RecordId(id),
			RelateThrough::Table(table) => Value::Table(table),
		}
	}
}

/// The reference back-link range one lookup subject reaches.
fn reference_lookup_range(
	this: &ComputedLookupSubject,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	id: &RecordIdKey,
) -> Result<TypedRange<()>> {
	match this {
		// Scan the entire range
		ComputedLookupSubject::Table {
			table,
			referencing_field: None,
		} => ReferenceForeignTablePrefix {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			id: Cow::Borrowed(id),
			foreign_table: Cow::Borrowed(table),
		}
		.range(),
		// Scan the entire range with a referencing field
		ComputedLookupSubject::Table {
			table,
			referencing_field: Some(field),
		} => ReferenceForeignFieldPrefix {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			id: Cow::Borrowed(id),
			foreign_table: Cow::Borrowed(table),
			foreign_field: Cow::Borrowed(field),
		}
		.range(),
		// Scan a specific range
		ComputedLookupSubject::Range {
			table,
			range,
			referencing_field,
		} => {
			let Some(field) = referencing_field else {
				bail!(
					"Cannot scan a specific range of record references without a referencing field"
				);
			};

			ReferenceForeignFieldPrefix {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				id: Cow::Borrowed(id),
				foreign_table: Cow::Borrowed(table),
				foreign_field: Cow::Borrowed(field),
			}
			.range_where((
				range.start.as_ref().map(Cow::Borrowed),
				range.end.as_ref().map(Cow::Borrowed),
			))
		}
	}
}

/// One graph adjacency slice a lookup subject reaches in one direction: the
/// vertex-side key range plus the fields the merged reader mirrors onto the
/// packed-block side.
struct GraphLookupScope {
	/// `None` spans both directions in one key-ordered scan.
	dir: Option<Dir>,
	edge_table: Option<TableName>,
	fk_lower: Option<RecordIdKey>,
	delta_range: RawRange,
}

/// The adjacency slice one lookup subject reaches in `dir`.
fn graph_lookup_scope(
	this: &ComputedLookupSubject,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	id: &RecordIdKey,
	dir: Dir,
) -> Result<GraphLookupScope> {
	let table = match this {
		ComputedLookupSubject::Table {
			table,
			..
		}
		| ComputedLookupSubject::Range {
			table,
			..
		} => table,
	};
	let prefix = GraphForeignTablePrefix {
		ns,
		db,
		tb: Cow::Borrowed(tb),
		id: Cow::Borrowed(id),
		dir,
		foreign_table: Cow::Borrowed(table),
	};
	let (delta_range, fk_lower) = match this {
		// Scan the entire range
		ComputedLookupSubject::Table {
			table: _,
			..
		} => (prefix.range()?, None),
		// Scan a specific range. An edge key is a prefix of the pointer keys
		// that extend it, so a bound on the foreign key covers that whole run.
		ComputedLookupSubject::Range {
			range,
			..
		} => {
			let fk_lower = match &range.start {
				Bound::Included(key) | Bound::Excluded(key) => Some(key.clone()),
				Bound::Unbounded => None,
			};
			(
				prefix.range_where((
					range.start.as_ref().map(Cow::Borrowed),
					range.end.as_ref().map(Cow::Borrowed),
				))?,
				fk_lower,
			)
		}
	};
	Ok(GraphLookupScope {
		dir: Some(dir),
		edge_table: Some(table.clone()),
		fk_lower,
		delta_range,
	})
}
