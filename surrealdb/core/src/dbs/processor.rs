use std::borrow::Cow;
use std::ops::{Bound, Range};
use std::sync::Arc;
use std::vec;

use anyhow::{Result, bail};
use futures::StreamExt;
use reblessive::tree::Stk;

use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, Record};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::distinct::SyncDistinct;
use crate::dbs::{Iterable, Iterator, Operable, Options, Processable, Statement};
use crate::doc::{DocumentContext, NsDbCtx, NsDbTbCtx};
use crate::err::Error;
use crate::expr::dir::Dir;
use crate::expr::lookup::{ComputedLookupSubject, LookupKind};
use crate::expr::statements::relate::RelateThrough;
use crate::idx::planner::iterators::{IndexItemRecord, IteratorRef, RecordIterator};
use crate::idx::planner::{IterationStage, RecordStrategy, ScanDirection};
use crate::key::{graph, record, r#ref};
use crate::kvs::{KVKey, KVValue, Key, Transaction, Val};
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
	Lookup(NsDbTbCtx, LookupKind, Key),
	RangeKey(NsDbTbCtx, Key),
	TableKey(NsDbTbCtx, Key),
	Relatable {
		doc_ctx: NsDbTbCtx,
		f: RecordId,
		v: RelateThrough,
		w: RecordId,
		o: Option<Value>,
	},
	RecordId(NsDbTbCtx, RecordId),
	GenerateRecordId(NsDbTbCtx, TableName),
	Value(NsDbCtx, Value),
	Defer(NsDbTbCtx, RecordId),
	Mergeable(NsDbTbCtx, TableName, Option<RecordIdKey>, Value),
	KeyVal(NsDbTbCtx, Key, Val),
	Count(NsDbTbCtx, usize),
	IndexItem(NsDbTbCtx, IndexItemRecord),
	IndexItemKey(NsDbTbCtx, IndexItemRecord),
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
		opt: &Options,
		txn: &Transaction,
		rid_only: bool,
	) -> Result<Processable> {
		match self {
			// Graph edge traversal results - requires special graph parsing and record lookup
			Self::Lookup(doc_ctx, kind, key) => {
				Self::process_lookup(doc_ctx, txn, kind, key, rid_only).await
			}
			// Range scan results - lightweight processing for range queries
			Self::RangeKey(doc_ctx, key) => Self::process_range_key(doc_ctx, key).await,
			// Table scan results - basic key-only processing for full table scans
			Self::TableKey(doc_ctx, key) => Self::process_table_key(doc_ctx, key).await,
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
			Self::KeyVal(doc_ctx, key, val) => Ok(Self::process_key_val(doc_ctx, key, val)?),
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
		doc_ctx: NsDbTbCtx,
		txn: &Transaction,
		kind: LookupKind,
		key: Key,
		rid_only: bool,
	) -> Result<Processable> {
		// Parse the data from the store
		let (ft, fk) = match kind {
			LookupKind::Graph(_) => {
				let gra = graph::Graph::decode_key(&key)?;
				(gra.ft, gra.fk)
			}
			LookupKind::Reference => {
				let refe = r#ref::Ref::decode_key(&key)?;
				(refe.ft, refe.fk)
			}
		};

		// Fetch the data from the store
		let record = if rid_only {
			Arc::new(Default::default())
		} else {
			txn.get_record(doc_ctx.ns.namespace_id, doc_ctx.db.database_id, ft.as_ref(), &fk, None)
				.await?
		};
		let rid = RecordId {
			table: ft.into_owned(),
			key: fk.into_owned(),
		};
		// Parse the data from the store
		let val = Operable::Value(record);
		// Process the record
		Ok(Processable {
			doc_ctx: DocumentContext::NsDbTbCtx(doc_ctx),
			record_strategy: RecordStrategy::KeysAndValues,
			generate: None,
			rid: Some(rid.into()),
			ir: None,
			val,
		})
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_range_key(doc_ctx: NsDbTbCtx, key: Key) -> Result<Processable> {
		let key = record::RecordKey::decode_key(&key)?;
		let val = Record::new(Value::Null);
		let rid = RecordId {
			table: key.tb.into_owned(),
			key: key.id,
		};
		// Create a new operable value
		let val = Operable::Value(val.into());
		// Process the record
		let pro = Processable {
			doc_ctx: DocumentContext::NsDbTbCtx(doc_ctx),
			record_strategy: RecordStrategy::KeysOnly,
			generate: None,
			rid: Some(rid.into()),
			ir: None,
			val,
		};
		Ok(pro)
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_table_key(doc_ctx: NsDbTbCtx, key: Key) -> Result<Processable> {
		let key = record::RecordKey::decode_key(&key)?;
		let rid = RecordId {
			table: key.tb.into_owned(),
			key: key.id,
		};
		// Process the record
		let pro = Processable {
			doc_ctx: DocumentContext::NsDbTbCtx(doc_ctx),
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
		doc_ctx: NsDbTbCtx,
		txn: &Transaction,
		f: RecordId,
		through: RelateThrough,
		w: RecordId,
		o: Option<Value>,
		rid_only: bool,
	) -> Result<Processable> {
		let pro = match (rid_only, through) {
			(true, RelateThrough::Table(v)) => Processable {
				doc_ctx: DocumentContext::NsDbTbCtx(doc_ctx),
				record_strategy: RecordStrategy::KeysOnly,
				generate: Some(v),
				rid: None,
				ir: None,
				val: Operable::Value(Default::default()),
			},
			(false, RelateThrough::Table(v)) => Processable {
				doc_ctx: DocumentContext::NsDbTbCtx(doc_ctx),
				record_strategy: RecordStrategy::KeysAndValues,
				generate: Some(v),
				rid: None,
				ir: None,
				val: Operable::Relate(f, Default::default(), w, o.map(|v| v.into())),
			},
			(true, RelateThrough::RecordId(v)) => Processable {
				doc_ctx: DocumentContext::NsDbTbCtx(doc_ctx),
				record_strategy: RecordStrategy::KeysOnly,
				generate: None,
				rid: Some(v.into()),
				ir: None,
				val: Operable::Value(Default::default()),
			},
			(false, RelateThrough::RecordId(v)) => {
				let val = txn
					.get_record(
						doc_ctx.ns.namespace_id,
						doc_ctx.db.database_id,
						&v.table,
						&v.key,
						None,
					)
					.await?;
				let val = Operable::Relate(f, val, w, o.map(|v| v.into()));

				Processable {
					doc_ctx: DocumentContext::NsDbTbCtx(doc_ctx),
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
		doc_ctx: NsDbTbCtx,
		txn: &Transaction,
		record_id: RecordId,
		rid_only: bool,
	) -> Result<Processable> {
		// if it is skippable we only need the record id
		let val = if rid_only {
			Record::new(Value::Null).into_read_only()
		} else {
			txn.get_record(
				doc_ctx.ns.namespace_id,
				doc_ctx.db.database_id,
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
			doc_ctx: DocumentContext::NsDbTbCtx(doc_ctx),
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
	async fn process_yield(doc_ctx: NsDbTbCtx, table_name: TableName) -> Result<Processable> {
		// Pass the value through
		let pro = Processable {
			doc_ctx: DocumentContext::NsDbTbCtx(doc_ctx),
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
	async fn process_defer(doc_ctx: NsDbTbCtx, v: RecordId) -> Result<Processable> {
		// Process the document record
		let pro = Processable {
			doc_ctx: DocumentContext::NsDbTbCtx(doc_ctx),
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
		doc_ctx: NsDbTbCtx,
		tb: TableName,
		id: Option<RecordIdKey>,
		o: Value,
	) -> Result<Processable> {
		// Process the document record
		let pro = if let Some(id) = id {
			Processable {
				doc_ctx: DocumentContext::NsDbTbCtx(doc_ctx),
				record_strategy: RecordStrategy::KeysAndValues,
				generate: None,
				rid: Some(RecordId::new(tb, id).into()),
				ir: None,
				val: Operable::Insert(Default::default(), o.into()),
			}
		} else {
			Processable {
				doc_ctx: DocumentContext::NsDbTbCtx(doc_ctx),
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
	fn process_key_val(doc_ctx: NsDbTbCtx, key: Key, val: Val) -> Result<Processable> {
		let key = record::RecordKey::decode_key(&key)?;
		let mut val = Record::kv_decode_value(val)?;
		let rid = RecordId {
			table: key.tb.into_owned(),
			key: key.id,
		};
		// Inject the id field into the document
		val.data.def(rid.clone());
		// Create a new operable value
		let val = Operable::Value(val.into());
		// Process the record
		Ok(Processable {
			doc_ctx: DocumentContext::NsDbTbCtx(doc_ctx),
			record_strategy: RecordStrategy::KeysAndValues,
			generate: None,

			rid: Some(rid.into()),
			ir: None,
			val,
		})
	}

	#[instrument(level = "trace", skip_all)]
	fn process_count(doc_ctx: NsDbTbCtx, count: usize) -> Processable {
		Processable {
			record_strategy: RecordStrategy::Count,
			generate: None,
			doc_ctx: DocumentContext::NsDbTbCtx(doc_ctx),
			rid: None,
			ir: None,
			val: Operable::Count(count),
		}
	}

	#[instrument(level = "trace", skip_all)]
	fn process_index_item_key(doc_ctx: NsDbTbCtx, i: IndexItemRecord) -> Processable {
		let (t, v, ir) = i.consume();
		Processable {
			record_strategy: RecordStrategy::KeysOnly,
			generate: None,
			doc_ctx: DocumentContext::NsDbTbCtx(doc_ctx),
			rid: Some(t),
			ir: Some(Arc::new(ir)),
			val: Operable::Value(v.unwrap_or_else(|| Record::new(Value::Null).into_read_only())),
		}
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_index_item(
		doc_ctx: NsDbTbCtx,
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
			txn.get_record(doc_ctx.ns.namespace_id, doc_ctx.db.database_id, &t.table, &t.key, None)
				.await?
		};
		let pro = Processable {
			doc_ctx: DocumentContext::NsDbTbCtx(doc_ctx),
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

		let pro = collectable.prepare(self.opt, self.txn, false).await?;
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
		let pro = collectable.prepare(self.coll.opt, self.coll.txn, skippable).await?;
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

	fn max_fetch_size(&mut self, normal_fetch_size: u32) -> u32 {
		if let Some(l) = self.iterator().start_limit() {
			*l
		} else {
			normal_fetch_size
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
			let mut ctx = Context::new(ctx);
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
					let mut ctx = Context::new(ctx);
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
	async fn start_skip(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		mut rng: Range<Key>,
		sc: ScanDirection,
	) -> Result<Option<Range<Key>>> {
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
		let mut stream = txn.stream_keys(rng.clone(), opt.version, Some(skippable), 0, sc);
		let mut skipped = 0;
		let mut last_key = vec![];
		'outer: while let Some(res) = stream.next().await {
			let batch = res?;
			for key in batch {
				if ctx.is_done(Some(skipped)).await? {
					break 'outer;
				}
				last_key = key;
				skipped += 1;
			}
		}
		// If we don't have a last key, we're done
		if last_key.is_empty() {
			return Ok(None);
		}
		// Update the iterator about the number of skipped keys
		ite.skipped(skipped);
		// We set the range for the next iteration
		match sc {
			ScanDirection::Forward => {
				last_key.push(0xFF);
				rng.start = last_key;
			}
			ScanDirection::Backward => {
				rng.end = last_key;
			}
		}
		Ok(Some(rng))
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_table(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		doc_ctx: NsDbTbCtx,
		table: &TableName,
		sc: ScanDirection,
	) -> Result<()> {
		let ns = doc_ctx.ns.namespace_id;
		let db = doc_ctx.db.database_id;

		// Prepare the start and end keys
		let beg = record::prefix(ns, db, table)?;
		let end = record::suffix(ns, db, table)?;

		// Optionally skip keys
		let Some(rng) = self.start_skip(ctx, opt, beg..end, sc).await? else {
			return Ok(());
		};

		// Create a new iterable range
		let txn = ctx.tx();
		let mut stream = txn.stream_keys_vals(rng, opt.version, None, 0, sc, false);

		// Loop until no more entries
		let mut count = 0;
		'outer: while let Some(res) = stream.next().await {
			let batch = res?;
			for (k, v) in batch {
				// Check if the context is finished
				if ctx.is_done(Some(count)).await? {
					break 'outer;
				}
				// Parse the data from the store
				self.collect(Collectable::KeyVal(doc_ctx.clone(), k, v)).await?;
				count += 1;
			}
		}
		// Everything ok
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_table_keys(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		doc_ctx: NsDbTbCtx,
		table: &TableName,
		sc: ScanDirection,
	) -> Result<()> {
		let ns = doc_ctx.ns.namespace_id;
		let db = doc_ctx.db.database_id;

		// Prepare the start and end keys
		let beg = record::prefix(ns, db, table)?;
		let end = record::suffix(ns, db, table)?;
		// Optionally skip keys
		let rng = if let Some(rng) = self.start_skip(ctx, opt, beg..end, sc).await? {
			// Returns the next range of keys
			rng
		} else {
			// There is nothing left to iterate
			return Ok(());
		};
		// Create a new iterable range
		let txn = ctx.tx();
		let mut stream = txn.stream_keys(rng, opt.version, None, 0, sc);
		// Loop until no more entries
		let mut count = 0;
		'outer: while let Some(res) = stream.next().await {
			let batch = res?;
			for k in batch {
				// Check if the context is finished
				if ctx.is_done(Some(count)).await? {
					break 'outer;
				}
				// Collect the key
				self.collect(Collectable::TableKey(doc_ctx.clone(), k)).await?;
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
		doc_ctx: NsDbTbCtx,
		v: &TableName,
	) -> Result<()> {
		let ns = doc_ctx.ns.namespace_id;
		let db = doc_ctx.db.database_id;
		let beg = record::prefix(ns, db, v)?;
		let end = record::suffix(ns, db, v)?;
		// Create a new iterable range
		let count = ctx.tx().count(beg..end, opt.version).await?;
		// Collect the count
		self.collect(Collectable::Count(doc_ctx, count)).await?;
		// Everything ok
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn range_prepare(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		r: RecordIdKeyRange,
	) -> Result<(Vec<u8>, Vec<u8>)> {
		let beg = match &r.start {
			Bound::Unbounded => record::prefix(ns, db, tb)?,
			Bound::Included(v) => record::new(ns, db, tb, v).encode_key()?,
			Bound::Excluded(v) => {
				let mut key = record::new(ns, db, tb, v).encode_key()?;
				key.push(0x00);
				key
			}
		};
		// Prepare the range end key
		let end = match &r.end {
			Bound::Unbounded => record::suffix(ns, db, tb)?,
			Bound::Excluded(v) => record::new(ns, db, tb, v).encode_key()?,
			Bound::Included(v) => {
				let mut key = record::new(ns, db, tb, v).encode_key()?;
				key.push(0x00);
				key
			}
		};
		Ok((beg, end))
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_range(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		doc_ctx: NsDbTbCtx,
		table_name: &TableName,
		r: RecordIdKeyRange,
		sc: ScanDirection,
	) -> Result<()> {
		let ns = doc_ctx.ns.namespace_id;
		let db = doc_ctx.db.database_id;
		// Prepare
		let (beg, end) = Self::range_prepare(ns, db, table_name, r).await?;
		// Optionally skip keys
		let rng = if let Some(rng) = self.start_skip(ctx, opt, beg..end, sc).await? {
			// Returns the next range of keys
			rng
		} else {
			// There is nothing left to iterate
			return Ok(());
		};
		// Create a new iterable range
		let txn = ctx.tx();
		let mut stream = txn.stream_keys_vals(rng, None, None, 0, sc, false);
		// Loop until no more entries
		let mut count = 0;
		'outer: while let Some(res) = stream.next().await {
			let batch = res?;
			for (k, v) in batch {
				// Check if the context is finished
				if ctx.is_done(Some(count)).await? {
					break 'outer;
				}
				// Collect
				self.collect(Collectable::KeyVal(doc_ctx.clone(), k, v)).await?;
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
		doc_ctx: NsDbTbCtx,
		tb: &TableName,
		r: RecordIdKeyRange,
		sc: ScanDirection,
	) -> Result<()> {
		let ns = doc_ctx.ns.namespace_id;
		let db = doc_ctx.db.database_id;

		// Get the transaction
		let txn = ctx.tx();
		// Prepare
		let (beg, end) = Self::range_prepare(ns, db, tb, r).await?;
		// Optionally skip keys
		let rng = if let Some(rng) = self.start_skip(ctx, opt, beg..end, sc).await? {
			// Returns the next range of keys
			rng
		} else {
			// There is nothing left to iterate
			return Ok(());
		};
		// Create a new iterable range
		let mut stream = txn.stream_keys(rng, opt.version, None, 0, sc);
		// Loop until no more entries
		let mut count = 0;
		'outer: while let Some(res) = stream.next().await {
			let batch = res?;
			for k in batch {
				// Check if the context is finished
				if ctx.is_done(Some(count)).await? {
					break 'outer;
				}
				// Collect the key
				self.collect(Collectable::RangeKey(doc_ctx.clone(), k)).await?;
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
		doc_ctx: NsDbTbCtx,
		tb: &TableName,
		r: RecordIdKeyRange,
	) -> Result<()> {
		// Get the transaction
		let txn = ctx.tx();
		// Prepare
		let (beg, end) =
			Self::range_prepare(doc_ctx.ns.namespace_id, doc_ctx.db.database_id, tb, r).await?;
		// Create a new iterable range
		let count = txn.count(beg..end, opt.version).await?;
		// Collect the count
		self.collect(Collectable::Count(doc_ctx, count)).await?;
		// Everything ok
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_lookup(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		doc_ctx: NsDbTbCtx,
		from: RecordId,
		kind: LookupKind,
		what: Vec<ComputedLookupSubject>,
	) -> Result<()> {
		let ns = doc_ctx.ns.namespace_id;
		let db = doc_ctx.db.database_id;

		// Pull out options
		let tb = &from.table;
		let id = &from.key;
		// Fetch start and end key pairs
		let keys = match (what.is_empty(), &kind) {
			(true, LookupKind::Reference) => {
				vec![(r#ref::prefix(ns, db, tb, id)?, r#ref::suffix(ns, db, tb, id)?)]
			}
			(true, LookupKind::Graph(dir)) => match dir {
				// /ns/db/tb/id
				Dir::Both => {
					vec![(graph::prefix(ns, db, tb, id)?, graph::suffix(ns, db, tb, id)?)]
				}
				// /ns/db/tb/id/IN
				Dir::In => vec![(
					graph::egprefix(ns, db, tb, id, dir)?,
					graph::egsuffix(ns, db, tb, id, dir)?,
				)],
				// /ns/db/tb/id/OUT
				Dir::Out => vec![(
					graph::egprefix(ns, db, tb, id, dir)?,
					graph::egsuffix(ns, db, tb, id, dir)?,
				)],
			},
			(false, LookupKind::Graph(Dir::Both)) => what
				.iter()
				.flat_map(|v| {
					[
						v.presuf(ns, db, tb, id, &LookupKind::Graph(Dir::In)),
						v.presuf(ns, db, tb, id, &LookupKind::Graph(Dir::Out)),
					]
				})
				.collect::<Result<Vec<_>>>()?,
			(false, kind) => {
				what.iter().map(|v| v.presuf(ns, db, tb, id, kind)).collect::<Result<Vec<_>>>()?
			}
		};
		// Get the transaction
		let txn = ctx.tx();
		// Loop over the chosen edge types
		'keys: for (beg, end) in keys {
			// Create a new iterable range
			let mut stream =
				txn.stream_keys(beg..end, opt.version, None, 0, ScanDirection::Forward);
			// Loop until no more entries
			let mut count = 0;
			while let Some(res) = stream.next().await {
				let batch = res?;
				for key in batch {
					// Check if the context is finished
					if ctx.is_done(Some(count)).await? {
						break 'keys;
					}
					// Collect the key
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
		doc_ctx: NsDbTbCtx,
		irf: IteratorRef,
		rs: RecordStrategy,
	) -> Result<()> {
		let Some(exe) = ctx.get_query_executor() else {
			bail!(Error::QueryNotExecuted {
				message: "No QueryExecutor has been found.".to_string(),
			})
		};

		let Some(iterator) =
			exe.new_iterator(doc_ctx.ns.namespace_id, doc_ctx.db.database_id, irf).await?
		else {
			bail!(Error::QueryNotExecuted {
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
		doc_ctx: NsDbTbCtx,
		mut iterator: RecordIterator,
	) -> Result<()> {
		let fetch_size = self.max_fetch_size(ctx.config().batching.normal_fetch_size);
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
		doc_ctx: NsDbTbCtx,
		mut iterator: RecordIterator,
	) -> Result<()> {
		let fetch_size = self.max_fetch_size(ctx.config().batching.normal_fetch_size);
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
		doc_ctx: NsDbTbCtx,
		mut iterator: RecordIterator,
	) -> Result<()> {
		let mut total_count = 0;
		let fetch_size = self.max_fetch_size(ctx.config().batching.normal_fetch_size);
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
