use crate::cnf::NORMAL_FETCH_SIZE;
use crate::ctx::{Context, MutableContext};
use crate::dbs::distinct::SyncDistinct;
use crate::dbs::{Iterable, Iterator, Operable, Options, Processed, Statement};
use crate::err::Error;
use crate::idx::planner::iterators::{IndexItemRecord, IteratorRef, ThingIterator};
use crate::idx::planner::{IterationStage, RecordStrategy, ScanDirection};
use crate::key::{graph, thing};
use crate::kvs::{Key, KeyDecode, KeyEncode, Transaction, Val};
use crate::sql::dir::Dir;
use crate::sql::id::range::IdRange;
use crate::sql::{Edges, Table, Thing, Value};
use futures::StreamExt;
use reblessive::tree::Stk;
use std::borrow::Cow;
use std::ops::{Bound, Range};
use std::sync::Arc;
use std::vec;

impl Iterable {
	pub(super) async fn iterate(
		self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		ite: &mut Iterator,
		dis: Option<&mut SyncDistinct>,
	) -> Result<(), Error> {
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

	fn iteration_stage_check(&self, ctx: &Context) -> bool {
		match self {
			Iterable::Table(tb, _, _) | Iterable::Index(tb, _, _) => {
				if let Some(IterationStage::BuildKnn) = ctx.get_iteration_stage() {
					if let Some(qp) = ctx.get_query_planner() {
						if let Some(exe) = qp.get_query_executor(tb) {
							return exe.has_bruteforce_knn();
						}
					}
				}
			}
			_ => {}
		}
		true
	}
}

pub(super) enum Collected {
	Edge(Key),
	RangeKey(Key),
	TableKey(Key),
	Relatable {
		f: Thing,
		v: Thing,
		w: Thing,
		o: Option<Value>,
	},
	Thing(Thing),
	Yield(Table),
	Value(Value),
	Defer(Thing),
	Mergeable(Thing, Value),
	KeyVal(Key, Val),
	Count(usize),
	IndexItem(IndexItemRecord),
	IndexItemKey(IndexItemRecord),
}

impl Collected {
	pub(super) async fn process(
		self,
		opt: &Options,
		txn: &Transaction,
		rid_only: bool,
	) -> Result<Processed, Error> {
		match self {
			Self::Edge(key) => Self::process_edge(opt, txn, key, rid_only).await,
			Self::RangeKey(key) => Self::process_range_key(key).await,
			Self::TableKey(key) => Self::process_table_key(key).await,
			Self::Relatable {
				f,
				v,
				w,
				o,
			} => Self::process_relatable(opt, txn, f, v, w, o, rid_only).await,
			Self::Thing(thing) => Self::process_thing(opt, txn, thing, rid_only).await,
			Self::Yield(table) => Self::process_yield(opt, txn, table, rid_only).await,
			Self::Value(value) => Ok(Self::process_value(value)),
			Self::Defer(key) => Self::process_defer(opt, txn, key, rid_only).await,
			Self::Mergeable(v, o) => Self::process_mergeable(opt, txn, v, o, rid_only).await,
			Self::KeyVal(key, val) => Ok(Self::process_key_val(key, val)?),
			Self::Count(c) => Ok(Self::process_count(c)),
			Self::IndexItem(i) => Self::process_index_item(opt, txn, i, rid_only).await,
			Self::IndexItemKey(i) => Ok(Self::process_index_item_key(i)),
		}
	}

	async fn process_edge(
		opt: &Options,
		txn: &Transaction,
		key: Key,
		rid_only: bool,
	) -> Result<Processed, Error> {
		// Parse the data from the store
		let gra: graph::Graph = graph::Graph::decode(&key)?;
		// Fetch the data from the store
		let val = if rid_only {
			Arc::new(Value::Null)
		} else {
			let (ns, db) = opt.ns_db()?;
			txn.get_record(ns, db, gra.ft, &gra.fk, None).await?
		};
		let rid = Thing::from((gra.ft, gra.fk));
		// Parse the data from the store
		let val = Operable::Value(val);
		// Process the record
		Ok(Processed {
			rs: RecordStrategy::KeysAndValues,
			generate: None,
			rid: Some(rid.into()),
			ir: None,
			val,
		})
	}

	async fn process_range_key(key: Key) -> Result<Processed, Error> {
		let key = thing::Thing::decode(&key)?;
		let val = Value::Null;
		let rid = Thing::from((key.tb, key.id));
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

	async fn process_table_key(key: Key) -> Result<Processed, Error> {
		let key = thing::Thing::decode(&key)?;
		let rid = Thing::from((key.tb, key.id));
		// Process the record
		let pro = Processed {
			rs: RecordStrategy::KeysOnly,
			generate: None,
			rid: Some(rid.into()),
			ir: None,
			val: Operable::Value(Value::Null.into()),
		};
		Ok(pro)
	}

	async fn process_relatable(
		opt: &Options,
		txn: &Transaction,
		f: Thing,
		v: Thing,
		w: Thing,
		o: Option<Value>,
		rid_only: bool,
	) -> Result<Processed, Error> {
		// if it is skippable we only need the record id
		let val = if rid_only {
			Operable::Value(Arc::new(Value::Null))
		} else {
			// Check that the table exists
			let (ns, db) = opt.ns_db()?;
			txn.check_ns_db_tb(ns, db, &v.tb, opt.strict).await?;
			// Fetch the data from the store
			let val = txn.get_record(ns, db, &v.tb, &v.id, None).await?;
			// Create a new operable value
			Operable::Relate(f, val, w, o.map(|v| v.into()))
		};
		// Process the document record
		let pro = Processed {
			rs: RecordStrategy::KeysAndValues,
			generate: None,
			rid: Some(v.into()),
			ir: None,
			val,
		};
		Ok(pro)
	}

	async fn process_thing(
		opt: &Options,
		txn: &Transaction,
		v: Thing,
		rid_only: bool,
	) -> Result<Processed, Error> {
		// if it is skippable we only need the record id
		let val = if rid_only {
			Arc::new(Value::Null)
		} else {
			// Check that the table exists
			let (ns, db) = opt.ns_db()?;
			txn.check_ns_db_tb(ns, db, &v.tb, opt.strict).await?;
			// Fetch the data from the store
			txn.get_record(ns, db, &v.tb, &v.id, opt.version).await?
		};
		// Parse the data from the store
		let val = Operable::Value(val);
		// Process the document record
		let pro = Processed {
			rs: RecordStrategy::KeysAndValues,
			generate: None,
			rid: Some(v.into()),
			ir: None,
			val,
		};
		// Everything ok
		Ok(pro)
	}

	async fn process_yield(
		opt: &Options,
		txn: &Transaction,
		v: Table,
		rid_only: bool,
	) -> Result<Processed, Error> {
		// if it is skippable we only need the record id
		if !rid_only {
			// Check that the table exists
			let (ns, db) = opt.ns_db()?;
			txn.check_ns_db_tb(ns, db, &v, opt.strict).await?;
		}
		// Pass the value through
		let pro = Processed {
			rs: RecordStrategy::KeysAndValues,
			generate: Some(v),
			rid: None,
			ir: None,
			val: Operable::Value(Value::None.into()),
		};
		Ok(pro)
	}

	fn process_value(v: Value) -> Processed {
		// Pass the value through
		Processed {
			rs: RecordStrategy::KeysAndValues,
			generate: None,
			rid: None,
			ir: None,
			val: Operable::Value(v.into()),
		}
	}

	async fn process_defer(
		opt: &Options,
		txn: &Transaction,
		v: Thing,
		rid_only: bool,
	) -> Result<Processed, Error> {
		// if it is skippable we only need the record id
		if !rid_only {
			// Check that the table exists
			let (ns, db) = opt.ns_db()?;
			txn.check_ns_db_tb(ns, db, &v.tb, opt.strict).await?;
		}
		// Process the document record
		let pro = Processed {
			rs: RecordStrategy::KeysAndValues,
			generate: None,
			rid: Some(v.into()),
			ir: None,
			val: Operable::Value(Value::None.into()),
		};
		Ok(pro)
	}

	async fn process_mergeable(
		opt: &Options,
		txn: &Transaction,
		v: Thing,
		o: Value,
		rid_only: bool,
	) -> Result<Processed, Error> {
		// if it is skippable we only need the record id
		if !rid_only {
			// Check that the table exists
			let (ns, db) = opt.ns_db()?;
			txn.check_ns_db_tb(ns, db, &v.tb, opt.strict).await?;
		}
		// Process the document record
		let pro = Processed {
			rs: RecordStrategy::KeysAndValues,
			generate: None,
			rid: Some(v.into()),
			ir: None,
			val: Operable::Insert(Value::None.into(), o.into()),
		};
		// Everything ok
		Ok(pro)
	}

	fn process_key_val(key: Key, val: Val) -> Result<Processed, Error> {
		let key = thing::Thing::decode(&key)?;
		let val: Value = revision::from_slice(&val)?;
		let rid = Thing::from((key.tb, key.id));
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

	fn process_count(count: usize) -> Processed {
		Processed {
			rs: RecordStrategy::Count,
			generate: None,
			rid: None,
			ir: None,
			val: Operable::Count(count),
		}
	}

	fn process_index_item_key(i: IndexItemRecord) -> Processed {
		let (t, v, ir) = i.consume();
		Processed {
			rs: RecordStrategy::KeysOnly,
			generate: None,
			rid: Some(t),
			ir: Some(Arc::new(ir)),
			val: Operable::Value(v.unwrap_or_else(|| Value::Null.into())),
		}
	}

	async fn process_index_item(
		opt: &Options,
		txn: &Transaction,
		i: IndexItemRecord,
		rid_only: bool,
	) -> Result<Processed, Error> {
		let (t, v, ir) = i.consume();
		let v = if let Some(v) = v {
			// The value may already be fetched by the KNN iterator to evaluate the condition
			v
		} else if rid_only {
			// if it is skippable we only need the record id
			Value::Null.into()
		} else {
			Iterable::fetch_thing(txn, opt, &t).await?
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
	ctx: &'a Context,
	opt: &'a Options,
	txn: &'a Transaction,
	stm: &'a Statement<'a>,
	ite: &'a mut Iterator,
}
impl Collector for ConcurrentCollector<'_> {
	async fn collect(&mut self, collected: Collected) -> Result<(), Error> {
		// if it is skippable don't need to process the document
		if self.ite.skippable() == 0 {
			let pro = collected.process(self.opt, self.txn, false).await?;
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
	async fn collect(&mut self, collected: Collected) -> Result<(), Error> {
		let skippable = self.coll.ite.skippable() > 0;
		// If it is skippable, we just need to collect the record id (if any)
		// to ensure that distinct can be checked.
		let pro = collected.process(self.coll.opt, self.coll.txn, skippable).await?;
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
	async fn collect(&mut self, collected: Collected) -> Result<(), Error>;

	fn iterator(&mut self) -> &mut Iterator;

	fn check_query_planner_context<'b>(ctx: &'b Context, table: &'b Table) -> Cow<'b, Context> {
		if let Some(qp) = ctx.get_query_planner() {
			if let Some(exe) = qp.get_query_executor(&table.0) {
				// We set the query executor matching the current table in the Context
				// Avoiding search in the hashmap of the query planner for each doc
				let mut ctx = MutableContext::new(ctx);
				ctx.set_query_executor(exe.clone());
				return Cow::Owned(ctx.freeze());
			}
		}
		Cow::Borrowed(ctx)
	}

	async fn collect_iterable(
		&mut self,
		ctx: &Context,
		opt: &Options,
		iterable: Iterable,
	) -> Result<(), Error> {
		if !ctx.is_done(None).await? {
			match iterable {
				Iterable::Value(v) => {
					if v.is_some() {
						return self.collect(Collected::Value(v)).await;
					}
				}
				Iterable::Yield(v) => self.collect(Collected::Yield(v)).await?,
				Iterable::Thing(v) => self.collect(Collected::Thing(v)).await?,
				Iterable::Defer(v) => self.collect(Collected::Defer(v)).await?,
				Iterable::Edges(e) => self.collect_edges(ctx, opt, e).await?,
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
					if let Some(qp) = ctx.get_query_planner() {
						if let Some(exe) = qp.get_query_executor(&v.0) {
							// We set the query executor matching the current table in the Context
							// Avoiding search in the hashmap of the query planner for each doc
							let mut ctx = MutableContext::new(ctx);
							ctx.set_query_executor(exe.clone());
							let ctx = ctx.freeze();
							return self.collect_index_items(&ctx, opt, &v, irf, rs).await;
						}
					}
					self.collect_index_items(ctx, opt, &v, irf, rs).await?
				}
				Iterable::Mergeable(v, o) => self.collect(Collected::Mergeable(v, o)).await?,
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

	async fn start_skip(
		&mut self,
		ctx: &Context,
		txn: &Transaction,
		mut rng: Range<Key>,
		sc: ScanDirection,
	) -> Result<Option<Range<Key>>, Error> {
		let ite = self.iterator();
		let skippable = ite.skippable();
		if skippable == 0 {
			// There is nothing to skip, we return the original range.
			return Ok(Some(rng));
		}
		// We only need to iterate over keys.
		let mut stream = txn.stream_keys(rng.clone(), Some(skippable), sc);
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
			#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
			ScanDirection::Backward => {
				rng.end = last_key;
			}
		}
		Ok(Some(rng))
	}

	async fn collect_table(
		&mut self,
		ctx: &Context,
		opt: &Options,
		v: &Table,
		sc: ScanDirection,
	) -> Result<(), Error> {
		// Get the transaction
		let txn = ctx.tx();
		// Check that the table exists
		let (ns, db) = opt.ns_db()?;
		txn.check_ns_db_tb(ns, db, v, opt.strict).await?;
		// Prepare the start and end keys
		let beg = thing::prefix(ns, db, v)?;
		let end = thing::suffix(ns, db, v)?;
		// Optionally skip keys
		let rng = if let Some(r) = self.start_skip(ctx, &txn, beg..end, sc).await? {
			r
		} else {
			return Ok(());
		};
		// Create a new iterable range
		let mut stream = txn.stream(rng, opt.version, None, sc);

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

	async fn collect_table_keys(
		&mut self,
		ctx: &Context,
		opt: &Options,
		v: &Table,
		sc: ScanDirection,
	) -> Result<(), Error> {
		// Get the transaction
		let txn = ctx.tx();
		// Check that the table exists
		let (ns, db) = opt.ns_db()?;
		txn.check_ns_db_tb(ns, db, v, opt.strict).await?;
		// Prepare the start and end keys
		let beg = thing::prefix(ns, db, v)?;
		let end = thing::suffix(ns, db, v)?;
		// Optionally skip keys
		let rng = if let Some(rng) = self.start_skip(ctx, &txn, beg..end, sc).await? {
			// Returns the next range of keys
			rng
		} else {
			// There is nothing left to iterate
			return Ok(());
		};
		// Create a new iterable range
		let mut stream = txn.stream_keys(rng, None, sc);
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

	async fn collect_table_count(
		&mut self,
		ctx: &Context,
		opt: &Options,
		v: &Table,
	) -> Result<(), Error> {
		// Get the transaction
		let txn = ctx.tx();
		// Check that the table exists
		let (ns, db) = opt.ns_db()?;
		txn.check_ns_db_tb(ns, db, v, opt.strict).await?;
		// Prepare the start and end keys
		let beg = thing::prefix(ns, db, v)?;
		let end = thing::suffix(ns, db, v)?;
		// Create a new iterable range
		let count = txn.count(beg..end).await?;
		// Collect the count
		self.collect(Collected::Count(count)).await?;
		// Everything ok
		Ok(())
	}

	async fn range_prepare(
		txn: &Transaction,
		opt: &Options,
		tb: &str,
		r: IdRange,
	) -> Result<(Vec<u8>, Vec<u8>), Error> {
		// Check that the table exists
		let (ns, db) = opt.ns_db()?;
		txn.check_ns_db_tb(ns, db, tb, opt.strict).await?;
		// Prepare the range start key
		let beg = match &r.beg {
			Bound::Unbounded => thing::prefix(ns, db, tb)?,
			Bound::Included(v) => thing::new(ns, db, tb, v).encode()?,
			Bound::Excluded(v) => {
				let mut key = thing::new(ns, db, tb, v).encode()?;
				key.push(0x00);
				key
			}
		};
		// Prepare the range end key
		let end = match &r.end {
			Bound::Unbounded => thing::suffix(ns, db, tb)?,
			Bound::Excluded(v) => thing::new(ns, db, tb, v).encode()?,
			Bound::Included(v) => {
				let mut key = thing::new(ns, db, tb, v).encode()?;
				key.push(0x00);
				key
			}
		};
		Ok((beg, end))
	}

	async fn collect_range(
		&mut self,
		ctx: &Context,
		opt: &Options,
		tb: &str,
		r: IdRange,
		sc: ScanDirection,
	) -> Result<(), Error> {
		// Get the transaction
		let txn = ctx.tx();
		// Prepare
		let (beg, end) = Self::range_prepare(&txn, opt, tb, r).await?;
		// Optionally skip keys
		let rng = if let Some(rng) = self.start_skip(ctx, &txn, beg..end, sc).await? {
			// Returns the next range of keys
			rng
		} else {
			// There is nothing left to iterate
			return Ok(());
		};
		// Create a new iterable range
		let mut stream = txn.stream(rng, None, None, sc);
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

	async fn collect_range_keys(
		&mut self,
		ctx: &Context,
		opt: &Options,
		tb: &str,
		r: IdRange,
		sc: ScanDirection,
	) -> Result<(), Error> {
		// Get the transaction
		let txn = ctx.tx();
		// Prepare
		let (beg, end) = Self::range_prepare(&txn, opt, tb, r).await?;
		// Optionally skip keys
		let rng = if let Some(rng) = self.start_skip(ctx, &txn, beg..end, sc).await? {
			// Returns the next range of keys
			rng
		} else {
			// There is nothing left to iterate
			return Ok(());
		};
		// Create a new iterable range
		let mut stream = txn.stream_keys(rng, None, sc);
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

	async fn collect_range_count(
		&mut self,
		ctx: &Context,
		opt: &Options,
		tb: &str,
		r: IdRange,
	) -> Result<(), Error> {
		// Get the transaction
		let txn = ctx.tx();
		// Prepare
		let (beg, end) = Self::range_prepare(&txn, opt, tb, r).await?;
		// Create a new iterable range
		let count = txn.count(beg..end).await?;
		// Collect the count
		self.collect(Collected::Count(count)).await?;
		// Everything ok
		Ok(())
	}

	async fn collect_edges(&mut self, ctx: &Context, opt: &Options, e: Edges) -> Result<(), Error> {
		// Pull out options
		let (ns, db) = opt.ns_db()?;
		let tb = &e.from.tb;
		let id = &e.from.id;
		// Fetch start and end key pairs
		let keys = match e.what.len() {
			0 => match e.dir {
				// /ns/db/tb/id
				Dir::Both => {
					vec![(graph::prefix(ns, db, tb, id), graph::suffix(ns, db, tb, id))]
				}
				// /ns/db/tb/id/IN
				Dir::In => vec![(
					graph::egprefix(ns, db, tb, id, &e.dir),
					graph::egsuffix(ns, db, tb, id, &e.dir),
				)],
				// /ns/db/tb/id/OUT
				Dir::Out => vec![(
					graph::egprefix(ns, db, tb, id, &e.dir),
					graph::egsuffix(ns, db, tb, id, &e.dir),
				)],
			},
			_ => match e.dir {
				// /ns/db/tb/id/IN/TB
				Dir::In => {
					e.what.iter().map(|v| v.presuf(ns, db, tb, id, &e.dir)).collect::<Vec<_>>()
				}
				// /ns/db/tb/id/OUT/TB
				Dir::Out => {
					e.what.iter().map(|v| v.presuf(ns, db, tb, id, &e.dir)).collect::<Vec<_>>()
				}
				// /ns/db/tb/id/IN/TB, /ns/db/tb/id/OUT/TB
				Dir::Both => e
					.what
					.iter()
					.flat_map(|v| {
						[v.presuf(ns, db, tb, id, &Dir::In), v.presuf(ns, db, tb, id, &Dir::Out)]
					})
					.collect::<Vec<_>>(),
			},
		};
		// Get the transaction
		let txn = ctx.tx();
		// Check that the table exists
		txn.check_ns_db_tb(ns, db, tb, opt.strict).await?;
		// Loop over the chosen edge types
		for (beg, end) in keys.into_iter() {
			// Create a new iterable range
			let mut stream = txn.stream(beg?..end?, None, None, ScanDirection::Forward);
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
				self.collect(Collected::Edge(key)).await?;
				count += 1;
			}
		}
		// Everything ok
		Ok(())
	}

	async fn collect_index_items(
		&mut self,
		ctx: &Context,
		opt: &Options,
		table: &Table,
		irf: IteratorRef,
		rs: RecordStrategy,
	) -> Result<(), Error> {
		// Check that the table exists
		let (ns, db) = opt.ns_db()?;
		ctx.tx().check_ns_db_tb(ns, db, &table.0, opt.strict).await?;
		if let Some(exe) = ctx.get_query_executor() {
			if let Some(iterator) = exe.new_iterator(opt, irf).await? {
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
				return Err(Error::QueryNotExecutedDetail {
					message: "No iterator has been found.".to_string(),
				});
			}
		}
		Err(Error::QueryNotExecutedDetail {
			message: "No QueryExecutor has been found.".to_string(),
		})
	}

	async fn collect_index_item_key(
		&mut self,
		ctx: &Context,
		txn: &Transaction,
		mut iterator: ThingIterator,
	) -> Result<(), Error> {
		while !ctx.is_done(None).await? {
			let records: Vec<IndexItemRecord> =
				iterator.next_batch(ctx, txn, *NORMAL_FETCH_SIZE).await?;
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

	async fn collect_index_item_key_value(
		&mut self,
		ctx: &Context,
		txn: &Transaction,
		mut iterator: ThingIterator,
	) -> Result<(), Error> {
		while !ctx.is_done(None).await? {
			let records: Vec<IndexItemRecord> =
				iterator.next_batch(ctx, txn, *NORMAL_FETCH_SIZE).await?;
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

	async fn collect_index_item_count(
		&mut self,
		ctx: &Context,
		txn: &Transaction,
		mut iterator: ThingIterator,
	) -> Result<(), Error> {
		let mut total_count = 0;
		while !ctx.is_done(None).await? {
			let count = iterator.next_count(ctx, txn, *NORMAL_FETCH_SIZE).await?;
			if count == 0 {
				break;
			}
			total_count += count;
		}
		self.collect(Collected::Count(total_count)).await
	}
}

impl Iterable {
	/// Returns the value from the store, or Value::None it the value does not exist.
	pub(crate) async fn fetch_thing(
		txn: &Transaction,
		opt: &Options,
		thg: &Thing,
	) -> Result<Arc<Value>, Error> {
		// Fetch and parse the data from the store
		let (ns, db) = opt.ns_db()?;
		let val = txn.get_record(ns, db, &thg.tb, &thg.id, None).await?;
		// Return the result
		Ok(val)
	}
}
