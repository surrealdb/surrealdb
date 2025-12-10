use std::borrow::Cow;
use std::ops::{Bound, Range};
use std::sync::Arc;
use std::vec;

use anyhow::{Result, bail};
use futures::StreamExt;
use reblessive::tree::Stk;

use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, Record};
use crate::cnf::NORMAL_FETCH_SIZE;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::distinct::SyncDistinct;
use crate::dbs::{Iterable, Iterator, Operable, Options, Processed, Statement};
use crate::err::Error;
use crate::expr::dir::Dir;
use crate::expr::lookup::{ComputedLookupSubject, LookupKind};
use crate::expr::statements::relate::RelateThrough;
use crate::idx::planner::iterators::{IndexItemRecord, IteratorRef, ThingIterator};
use crate::idx::planner::{IterationStage, RecordStrategy, ScanDirection};
use crate::key::{graph, record, r#ref};
use crate::kvs::{KVKey, KVValue, Key, Transaction, Val};
use crate::val::{RecordId, RecordIdKey, RecordIdKeyRange, Value};

impl Iterable {
	pub(super) async fn iterate(
		self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
		ite: &mut Iterator,
		dis: Option<&mut SyncDistinct>,
	) -> Result<()> {
		if self.iteration_stage_check(ctx) {
			let txn = ctx.tx();
			let mut coll = ConcurrentCollector {
				stk,
				ctx,
				opt,
				txn: &txn,
				stm,
				ite,
			};
			if let Some(dis) = dis {
				let mut coll = ConcurrentDistinctCollector {
					coll,
					dis,
				};
				coll.collect_iterable(ctx, opt, self).await?;
			} else {
				coll.collect_iterable(ctx, opt, self).await?;
			}
		}
		Ok(())
	}

	fn iteration_stage_check(&self, ctx: &FrozenContext) -> bool {
		match self {
			Iterable::Table(tb, _, _) | Iterable::Index(tb, _, _) => {
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

pub(super) enum Collected {
	Lookup(LookupKind, Key),
	RangeKey(Key),
	TableKey(Key),
	Relatable {
		f: RecordId,
		v: RelateThrough,
		w: RecordId,
		o: Option<Value>,
	},
	RecordId(RecordId),
	Yield(String),
	Value(Value),
	Defer(RecordId),
	Mergeable(String, Option<RecordIdKey>, Value),
	KeyVal(Key, Val),
	Count(usize),
	IndexItem(IndexItemRecord),
	IndexItemKey(IndexItemRecord),
}

impl Collected {
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
	#[instrument(level = "trace", name = "Collected::process", skip_all)]
	pub(super) async fn process(
		self,
		ctx: &FrozenContext,
		opt: &Options,
		txn: &Transaction,
		rid_only: bool,
	) -> Result<Processed> {
		match self {
			// Graph edge traversal results - requires special graph parsing and record lookup
			Self::Lookup(kind, key) => {
				Self::process_lookup(ctx, opt, txn, kind, key, rid_only).await
			}
			// Range scan results - lightweight processing for range queries
			Self::RangeKey(key) => Self::process_range_key(key).await,
			// Table scan results - basic key-only processing for full table scans
			Self::TableKey(key) => Self::process_table_key(key).await,
			// Graph relationship records - handles complex from/via/to relationship processing
			Self::Relatable {
				f,
				v,
				w,
				o,
			} => Self::process_relatable(ctx, opt, txn, f, v, w, o, rid_only).await,
			// Direct record ID references - standard record processing
			Self::RecordId(record_id) => {
				Self::process_record(ctx, opt, txn, record_id, rid_only).await
			}
			// Table identifiers - used for table-level operations
			Self::Yield(table) => Self::process_yield(table).await,
			// Pre-computed values - no additional processing needed
			Self::Value(value) => Ok(Self::process_value(value)),
			// Deferred record processing - handles lazy evaluation scenarios
			Self::Defer(key) => Self::process_defer(key).await,
			// Records with merge operations - applies data merging logic
			Self::Mergeable(tb, id, o) => Self::process_mergeable(tb, id, o).await,
			// Raw key-value pairs from storage layer
			Self::KeyVal(key, val) => Ok(Self::process_key_val(key, val)?),
			// Count aggregation results - no record processing needed
			Self::Count(c) => Ok(Self::process_count(c)),
			// Index scan results with values - includes pre-fetched data
			Self::IndexItem(i) => Self::process_index_item(ctx, opt, txn, i, rid_only).await,
			// Index scan results key-only - lightweight index processing
			Self::IndexItemKey(i) => Ok(Self::process_index_item_key(i)),
		}
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_lookup(
		ctx: &FrozenContext,
		opt: &Options,
		txn: &Transaction,
		kind: LookupKind,
		key: Key,
		rid_only: bool,
	) -> Result<Processed> {
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
			let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
			txn.get_record(ns, db, ft.as_ref(), &fk, None).await?
		};
		let rid = RecordId {
			table: ft.into_owned(),
			key: fk.into_owned(),
		};
		// Parse the data from the store
		let val = Operable::Value(record);
		// Process the record
		Ok(Processed {
			rs: RecordStrategy::KeysAndValues,
			generate: None,
			rid: Some(rid.into()),
			ir: None,
			val,
		})
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_range_key(key: Key) -> Result<Processed> {
		let key = record::RecordKey::decode_key(&key)?;
		let val = Record::new(Value::Null.into());
		let rid = RecordId {
			table: key.tb.into_owned(),
			key: key.id,
		};
		// Create a new operable value
		let val = Operable::Value(val.into());
		// Process the record
		let pro = Processed {
			rs: RecordStrategy::KeysOnly,
			generate: None,
			rid: Some(rid.into()),
			ir: None,
			val,
		};
		Ok(pro)
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_table_key(key: Key) -> Result<Processed> {
		let key = record::RecordKey::decode_key(&key)?;
		let rid = RecordId {
			table: key.tb.into_owned(),
			key: key.id,
		};
		// Process the record
		let pro = Processed {
			rs: RecordStrategy::KeysOnly,
			generate: None,
			rid: Some(rid.into()),
			ir: None,
			val: Operable::Value(Record::new(Value::Null.into()).into_read_only()),
		};
		Ok(pro)
	}

	#[expect(clippy::too_many_arguments)]
	#[instrument(level = "trace", skip_all)]
	async fn process_relatable(
		ctx: &FrozenContext,
		opt: &Options,
		txn: &Transaction,
		f: RecordId,
		v: RelateThrough,
		w: RecordId,
		o: Option<Value>,
		rid_only: bool,
	) -> Result<Processed> {
		let pro = match (rid_only, v) {
			(true, RelateThrough::Table(v)) => Processed {
				rs: RecordStrategy::KeysOnly,
				generate: Some(v),
				rid: None,
				ir: None,
				val: Operable::Value(Default::default()),
			},
			(false, RelateThrough::Table(v)) => Processed {
				rs: RecordStrategy::KeysAndValues,
				generate: Some(v),
				rid: None,
				ir: None,
				val: Operable::Relate(f, Default::default(), w, None),
			},
			(true, RelateThrough::RecordId(v)) => Processed {
				rs: RecordStrategy::KeysOnly,
				generate: None,
				rid: Some(v.into()),
				ir: None,
				val: Operable::Value(Default::default()),
			},
			(false, RelateThrough::RecordId(v)) => {
				let (ns, db) = ctx.get_ns_db_ids(opt).await?;
				let val = txn.get_record(ns, db, &v.table, &v.key, None).await?;
				let val = Operable::Relate(f, val, w, o.map(|v| v.into()));

				Processed {
					rs: RecordStrategy::KeysAndValues,
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
		ctx: &FrozenContext,
		opt: &Options,
		txn: &Transaction,
		record_id: RecordId,
		rid_only: bool,
	) -> Result<Processed> {
		// if it is skippable we only need the record id
		let val = if rid_only {
			Record::new(Value::Null.into()).into_read_only()
		} else {
			let (ns, db) = ctx.get_ns_db_ids(opt).await?;
			txn.get_record(ns, db, &record_id.table, &record_id.key, opt.version).await?
		};
		// Parse the data from the store
		let val = Operable::Value(val);
		// Process the document record
		let pro = Processed {
			rs: RecordStrategy::KeysAndValues,
			generate: None,
			rid: Some(record_id.into()),
			ir: None,
			val,
		};
		// Everything ok
		Ok(pro)
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_yield(v: String) -> Result<Processed> {
		// Pass the value through
		let pro = Processed {
			rs: RecordStrategy::KeysAndValues,
			generate: Some(v),
			rid: None,
			ir: None,
			val: Operable::Value(Default::default()),
		};
		Ok(pro)
	}

	#[instrument(level = "trace", skip_all)]
	fn process_value(v: Value) -> Processed {
		// Try to extract the id field if present and parse as Thing
		let rid = match &v {
			Value::RecordId(thing) => Some(Arc::new(thing.clone())),
			_ => None,
		};
		Processed {
			rs: RecordStrategy::KeysAndValues,
			generate: None,
			rid,
			ir: None,
			val: Operable::Value(Record::new(v.into()).into_read_only()),
		}
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_defer(v: RecordId) -> Result<Processed> {
		// Process the document record
		let pro = Processed {
			rs: RecordStrategy::KeysAndValues,
			generate: None,
			rid: Some(v.into()),
			ir: None,
			val: Operable::Value(Default::default()),
		};
		Ok(pro)
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_mergeable(tb: String, id: Option<RecordIdKey>, o: Value) -> Result<Processed> {
		// Process the document record
		let pro = if let Some(id) = id {
			Processed {
				rs: RecordStrategy::KeysAndValues,
				generate: None,
				rid: Some(RecordId::new(tb, id).into()),
				ir: None,
				val: Operable::Insert(Default::default(), o.into()),
			}
		} else {
			Processed {
				rs: RecordStrategy::KeysOnly,
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
	fn process_key_val(key: Key, val: Val) -> Result<Processed> {
		let key = record::RecordKey::decode_key(&key)?;
		let mut val = Record::kv_decode_value(val)?;
		let rid = RecordId {
			table: key.tb.into_owned(),
			key: key.id,
		};
		// Inject the id field into the document
		val.data.to_mut().def(&rid);
		// Create a new operable value
		let val = Operable::Value(val.into());
		// Process the record
		Ok(Processed {
			rs: RecordStrategy::KeysAndValues,
			generate: None,
			rid: Some(rid.into()),
			ir: None,
			val,
		})
	}

	#[instrument(level = "trace", skip_all)]
	fn process_count(count: usize) -> Processed {
		Processed {
			rs: RecordStrategy::Count,
			generate: None,
			rid: None,
			ir: None,
			val: Operable::Count(count),
		}
	}

	#[instrument(level = "trace", skip_all)]
	fn process_index_item_key(i: IndexItemRecord) -> Processed {
		let (t, v, ir) = i.consume();
		Processed {
			rs: RecordStrategy::KeysOnly,
			generate: None,
			rid: Some(t),
			ir: Some(Arc::new(ir)),
			val: Operable::Value(
				v.unwrap_or_else(|| Record::new(Value::Null.into()).into_read_only()),
			),
		}
	}

	#[instrument(level = "trace", skip_all)]
	async fn process_index_item(
		ctx: &FrozenContext,
		opt: &Options,
		txn: &Transaction,
		i: IndexItemRecord,
		rid_only: bool,
	) -> Result<Processed> {
		let (t, v, ir) = i.consume();
		let v = if let Some(v) = v {
			// The value may already be fetched by the KNN iterator to evaluate the
			// condition
			v
		} else if rid_only {
			// if it is skippable we only need the record id
			Record::new(Value::Null.into()).into_read_only()
		} else {
			let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
			txn.get_record(ns, db, &t.table, &t.key, None).await?
		};
		let pro = Processed {
			rs: RecordStrategy::KeysAndValues,
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
	#[instrument(skip_all)]
	async fn collect(&mut self, collected: Collected) -> Result<()> {
		// if it is skippable don't need to process the document
		if self.ite.skippable() == 0 {
			let pro = collected.process(self.ctx, self.opt, self.txn, false).await?;
			self.ite.process(self.stk, self.ctx, self.opt, self.stm, pro).await?;
		} else {
			self.ite.skipped(1);
		}
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
	#[instrument(skip_all)]
	async fn collect(&mut self, collected: Collected) -> Result<()> {
		let skippable = self.coll.ite.skippable() > 0;
		// If it is skippable, we just need to collect the record id (if any)
		// to ensure that distinct can be checked.
		let pro = collected.process(self.coll.ctx, self.coll.opt, self.coll.txn, skippable).await?;
		if !self.dis.check_already_processed(&pro) {
			if !skippable {
				self.coll
					.ite
					.process(self.coll.stk, self.coll.ctx, self.coll.opt, self.coll.stm, pro)
					.await?;
			} else {
				self.coll.ite.skipped(1);
			}
		}
		Ok(())
	}

	fn iterator(&mut self) -> &mut Iterator {
		self.coll.ite
	}
}

pub(super) trait Collector {
	async fn collect(&mut self, collected: Collected) -> Result<()>;

	fn max_fetch_size(&mut self) -> u32 {
		if let Some(l) = self.iterator().start_limit() {
			*l
		} else {
			*NORMAL_FETCH_SIZE
		}
	}

	fn iterator(&mut self) -> &mut Iterator;

	fn check_query_planner_context<'b>(
		ctx: &'b FrozenContext,
		table: &'b str,
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

	#[instrument(level = "trace", skip_all)]
	async fn collect_iterable(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		iterable: Iterable,
	) -> Result<()> {
		if !ctx.is_done(None).await? {
			match iterable {
				Iterable::Value(v) => {
					if !v.is_nullish() {
						return self.collect(Collected::Value(v)).await;
					}
				}
				Iterable::Yield(v) => self.collect(Collected::Yield(v)).await?,
				Iterable::Thing(v) => self.collect(Collected::RecordId(v)).await?,
				Iterable::Defer(v) => self.collect(Collected::Defer(v)).await?,
				Iterable::Lookup {
					from,
					kind,
					what,
				} => self.collect_lookup(ctx, opt, from, kind, what).await?,
				// For Table and Range iterables, the RecordStrategy determines whether we
				// collect only keys, keys+values, or just a count without materializing records.
				Iterable::Range(tb, v, rs, sc) => match rs {
					RecordStrategy::Count => self.collect_range_count(ctx, opt, &tb, v).await?,
					RecordStrategy::KeysOnly => {
						self.collect_range_keys(ctx, opt, &tb, v, sc).await?
					}
					RecordStrategy::KeysAndValues => {
						self.collect_range(ctx, opt, &tb, v, sc).await?
					}
				},
				Iterable::Table(v, rs, sc) => {
					let ctx = Self::check_query_planner_context(ctx, &v);
					match rs {
						RecordStrategy::Count => self.collect_table_count(&ctx, opt, &v).await?,
						RecordStrategy::KeysOnly => {
							self.collect_table_keys(&ctx, opt, &v, sc).await?
						}
						RecordStrategy::KeysAndValues => {
							self.collect_table(&ctx, opt, &v, sc).await?
						}
					}
				}
				Iterable::Index(v, irf, rs) => {
					if let Some(qp) = ctx.get_query_planner()
						&& let Some(exe) = qp.get_query_executor(v.as_str())
					{
						// Attach the table-specific QueryExecutor to the Context to avoid
						// per-record lookups in the QueryPlanner during index scans.
						// This significantly reduces overhead inside tight iterator loops.
						let mut ctx = Context::new(ctx);
						ctx.set_query_executor(exe.clone());
						let ctx = ctx.freeze();
						return self.collect_index_items(&ctx, opt, irf, rs).await;
					}
					self.collect_index_items(ctx, opt, irf, rs).await?
				}
				Iterable::Mergeable(tb, id, o) => {
					self.collect(Collected::Mergeable(tb, id, o)).await?
				}
				Iterable::Relatable(f, v, w, o) => {
					self.collect(Collected::Relatable {
						f,
						v,
						w,
						o,
					})
					.await?
				}
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
		let mut stream = txn.stream_keys(rng.clone(), opt.version, Some(skippable), sc);
		let mut skipped = 0;
		let mut last_key = vec![];
		while let Some(res) = stream.next().await {
			if ctx.is_done(Some(skipped)).await? {
				break;
			}
			last_key = res?;
			skipped += 1;
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
		v: &str,
		sc: ScanDirection,
	) -> Result<()> {
		let db = ctx.get_db(opt).await?;

		// Get the transaction
		let txn = ctx.tx();
		if db.strict {
			txn.expect_tb(db.namespace_id, db.database_id, v).await?;
		}

		// Prepare the start and end keys
		let beg = record::prefix(db.namespace_id, db.database_id, v)?;
		let end = record::suffix(db.namespace_id, db.database_id, v)?;
		// Optionally skip keys
		let rng = if let Some(r) = self.start_skip(ctx, opt, beg..end, sc).await? {
			r
		} else {
			return Ok(());
		};
		// Create a new iterable range
		let mut stream = txn.stream_keys_vals(rng, opt.version, None, sc);

		// Loop until no more entries
		let mut count = 0;
		while let Some(res) = stream.next().await {
			// Check if the context is finished
			if ctx.is_done(Some(count)).await? {
				break;
			}
			// Parse the data from the store
			let (k, v) = res?;
			self.collect(Collected::KeyVal(k, v)).await?;
			count += 1;
		}
		// Everything ok
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_table_keys(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		v: &str,
		sc: ScanDirection,
	) -> Result<()> {
		let db = ctx.get_db(opt).await?;

		// Get the transaction
		let txn = ctx.tx();
		// Check that the table exists
		if db.strict {
			txn.expect_tb(db.namespace_id, db.database_id, v).await?;
		}

		// Prepare the start and end keys
		let beg = record::prefix(db.namespace_id, db.database_id, v)?;
		let end = record::suffix(db.namespace_id, db.database_id, v)?;
		// Optionally skip keys
		let rng = if let Some(rng) = self.start_skip(ctx, opt, beg..end, sc).await? {
			// Returns the next range of keys
			rng
		} else {
			// There is nothing left to iterate
			return Ok(());
		};
		// Create a new iterable range
		let mut stream = txn.stream_keys(rng, opt.version, None, sc);
		// Loop until no more entries
		let mut count = 0;
		while let Some(res) = stream.next().await {
			// Check if the context is finished
			if ctx.is_done(Some(count)).await? {
				break;
			}
			// Parse the data from the store
			let k = res?;
			// Collect the key
			self.collect(Collected::TableKey(k)).await?;
			count += 1;
		}
		// Everything ok
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_table_count(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		v: &str,
	) -> Result<()> {
		let db = ctx.get_db(opt).await?;

		// Get the transaction
		let txn = ctx.tx();
		// Check that the table exists
		if db.strict {
			txn.expect_tb(db.namespace_id, db.database_id, v).await?;
		}

		let beg = record::prefix(db.namespace_id, db.database_id, v)?;
		let end = record::suffix(db.namespace_id, db.database_id, v)?;
		// Create a new iterable range
		let count = txn.count(beg..end).await?;
		// Collect the count
		self.collect(Collected::Count(count)).await?;
		// Everything ok
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn range_prepare(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
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
		tb: &str,
		r: RecordIdKeyRange,
		sc: ScanDirection,
	) -> Result<()> {
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;

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
		let mut stream = txn.stream_keys_vals(rng, None, None, sc);
		// Loop until no more entries
		let mut count = 0;
		while let Some(res) = stream.next().await {
			// Check if the context is finished
			if ctx.is_done(Some(count)).await? {
				break;
			}
			// Parse the data from the store
			let (k, v) = res?;
			// Collect
			self.collect(Collected::KeyVal(k, v)).await?;
			count += 1;
		}
		// Everything ok
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_range_keys(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		tb: &str,
		r: RecordIdKeyRange,
		sc: ScanDirection,
	) -> Result<()> {
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;

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
		let mut stream = txn.stream_keys(rng, opt.version, None, sc);
		// Loop until no more entries
		let mut count = 0;
		while let Some(res) = stream.next().await {
			// Check if the context is finished
			if ctx.is_done(Some(count)).await? {
				break;
			}
			// Parse the data from the store
			let k = res?;
			self.collect(Collected::RangeKey(k)).await?;
			count += 1;
		}
		// Everything ok
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_range_count(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		tb: &str,
		r: RecordIdKeyRange,
	) -> Result<()> {
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;

		// Get the transaction
		let txn = ctx.tx();
		// Prepare
		let (beg, end) = Self::range_prepare(ns, db, tb, r).await?;
		// Create a new iterable range
		let count = txn.count(beg..end).await?;
		// Collect the count
		self.collect(Collected::Count(count)).await?;
		// Everything ok
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_lookup(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		from: RecordId,
		kind: LookupKind,
		what: Vec<ComputedLookupSubject>,
	) -> Result<()> {
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;

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
		// Check that the table exists
		// Loop over the chosen edge types
		for (beg, end) in keys.into_iter() {
			// Create a new iterable range
			let mut stream =
				txn.stream_keys_vals(beg..end, opt.version, None, ScanDirection::Forward);
			// Loop until no more entries
			let mut count = 0;
			while let Some(res) = stream.next().await {
				// Check if the context is finished
				if ctx.is_done(Some(count)).await? {
					break;
				}
				// Parse the key from the result
				let key = res?.0;
				// Collector the key
				self.collect(Collected::Lookup(kind.clone(), key)).await?;
				count += 1;
			}
		}
		// Everything ok
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_index_items(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		irf: IteratorRef,
		rs: RecordStrategy,
	) -> Result<()> {
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;

		if let Some(exe) = ctx.get_query_executor() {
			if let Some(iterator) = exe.new_iterator(ns, db, irf).await? {
				let txn = ctx.tx();
				match rs {
					RecordStrategy::Count => {
						self.collect_index_item_count(ctx, &txn, iterator).await?
					}
					RecordStrategy::KeysOnly => {
						self.collect_index_item_key(ctx, &txn, iterator).await?
					}
					RecordStrategy::KeysAndValues => {
						self.collect_index_item_key_value(ctx, &txn, iterator).await?
					}
				}
				// Everything ok
				return Ok(());
			} else {
				bail!(Error::QueryNotExecuted {
					message: "No iterator has been found.".to_string(),
				});
			}
		}
		bail!(Error::QueryNotExecuted {
			message: "No QueryExecutor has been found.".to_string(),
		})
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_index_item_key(
		&mut self,
		ctx: &FrozenContext,
		txn: &Transaction,
		mut iterator: ThingIterator,
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
				self.collect(Collected::IndexItemKey(record)).await?;
			}
		}
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_index_item_key_value(
		&mut self,
		ctx: &FrozenContext,
		txn: &Transaction,
		mut iterator: ThingIterator,
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
				self.collect(Collected::IndexItem(record)).await?;
			}
		}
		Ok(())
	}

	#[instrument(level = "trace", skip_all)]
	async fn collect_index_item_count(
		&mut self,
		ctx: &FrozenContext,
		txn: &Transaction,
		mut iterator: ThingIterator,
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
		self.collect(Collected::Count(total_count)).await
	}
}
