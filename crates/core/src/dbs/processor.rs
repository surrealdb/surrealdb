use crate::cnf::NORMAL_FETCH_SIZE;
use crate::ctx::{Context, MutableContext};
use crate::dbs::distinct::SyncDistinct;
use crate::dbs::{Iterable, Iterator, Operable, Options, Processed, Statement};
use crate::err::Error;
use crate::idx::planner::iterators::{CollectorRecord, IteratorRef};
use crate::idx::planner::IterationStage;
use crate::key::{graph, thing};
use crate::kvs::{Key, Transaction, Val};
use crate::sql::dir::Dir;
use crate::sql::id::range::IdRange;
use crate::sql::{Edges, Table, Thing, Value};
use futures::StreamExt;
use reblessive::tree::Stk;
use std::borrow::Cow;
use std::ops::Bound;
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
			Iterable::Table(tb, _) | Iterable::Index(tb, _) => {
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
	IndexItem(CollectorRecord),
}

impl Collected {
	pub(super) async fn process(
		self,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Processed, Error> {
		match self {
			Self::Edge(key) => Self::process_edge(opt, txn, key).await,
			Self::RangeKey(key) => Self::process_range_key(key).await,
			Self::TableKey(key) => Self::process_table_key(key).await,
			Self::Relatable {
				f,
				v,
				w,
				o,
			} => Self::process_relatable(opt, txn, f, v, w, o).await,
			Self::Thing(thing) => Self::process_thing(opt, txn, thing).await,
			Self::Yield(table) => Self::process_yield(opt, txn, table).await,
			Self::Value(value) => Self::process_value(value),
			Self::Defer(key) => Self::process_defer(opt, txn, key).await,
			Self::Mergeable(v, o) => Self::process_mergeable(opt, txn, v, o).await,
			Self::KeyVal(key, val) => Self::process_key_val(key, val).await,
			Self::IndexItem(r) => Self::process_index_item(opt, txn, r).await,
		}
	}

	async fn process_edge(opt: &Options, txn: &Transaction, key: Key) -> Result<Processed, Error> {
		// Parse the data from the store
		let gra: graph::Graph = graph::Graph::decode(&key)?;
		// Fetch the data from the store
		let val = txn.get_record(opt.ns()?, opt.db()?, gra.ft, &gra.fk, None).await?;
		let rid = Thing::from((gra.ft, gra.fk));
		// Parse the data from the store
		let val = Operable::Value(val);
		// Process the record
		Ok(Processed {
			keys_only: false,
			generate: None,
			rid: Some(rid.into()),
			ir: None,
			val,
		})
	}

	async fn process_range_key(key: Key) -> Result<Processed, Error> {
		let key: thing::Thing = (&key).into();
		let val = Value::Null;
		let rid = Thing::from((key.tb, key.id));
		// Create a new operable value
		let val = Operable::Value(val.into());
		// Process the record
		let pro = Processed {
			keys_only: true,
			generate: None,
			rid: Some(rid.into()),
			ir: None,
			val,
		};
		Ok(pro)
	}

	async fn process_table_key(key: Key) -> Result<Processed, Error> {
		let key: thing::Thing = (&key).into();
		let rid = Thing::from((key.tb, key.id));
		// Process the record
		let pro = Processed {
			keys_only: true,
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
	) -> Result<Processed, Error> {
		// Check that the table exists
		txn.check_ns_db_tb(opt.ns()?, opt.db()?, &v.tb, opt.strict).await?;
		// Fetch the data from the store
		let val = txn.get_record(opt.ns()?, opt.db()?, &v.tb, &v.id, None).await?;
		// Create a new operable value
		let val = Operable::Relate(f, val, w, o.map(|v| v.into()));
		// Process the document record
		let pro = Processed {
			keys_only: false,
			generate: None,
			rid: Some(v.into()),
			ir: None,
			val,
		};
		Ok(pro)
	}

	async fn process_thing(opt: &Options, txn: &Transaction, v: Thing) -> Result<Processed, Error> {
		// Check that the table exists
		txn.check_ns_db_tb(opt.ns()?, opt.db()?, &v.tb, opt.strict).await?;
		// Fetch the data from the store
		let val = txn.get_record(opt.ns()?, opt.db()?, &v.tb, &v.id, opt.version).await?;
		// Parse the data from the store
		let val = Operable::Value(val);
		// Process the document record
		let pro = Processed {
			keys_only: false,
			generate: None,
			rid: Some(v.into()),
			ir: None,
			val,
		};
		// Everything ok
		Ok(pro)
	}

	async fn process_yield(opt: &Options, txn: &Transaction, v: Table) -> Result<Processed, Error> {
		// Check that the table exists
		txn.check_ns_db_tb(opt.ns()?, opt.db()?, &v, opt.strict).await?;
		// Pass the value through
		let pro = Processed {
			keys_only: false,
			generate: Some(v),
			rid: None,
			ir: None,
			val: Operable::Value(Value::None.into()),
		};
		Ok(pro)
	}

	fn process_value(v: Value) -> Result<Processed, Error> {
		// Pass the value through
		let pro = Processed {
			keys_only: false,
			generate: None,
			rid: None,
			ir: None,
			val: Operable::Value(v.into()),
		};
		Ok(pro)
	}

	async fn process_defer(opt: &Options, txn: &Transaction, v: Thing) -> Result<Processed, Error> {
		// Check that the table exists
		txn.check_ns_db_tb(opt.ns()?, opt.db()?, &v.tb, opt.strict).await?;
		// Process the document record
		let pro = Processed {
			keys_only: false,
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
	) -> Result<Processed, Error> {
		// Check that the table exists
		txn.check_ns_db_tb(opt.ns()?, opt.db()?, &v.tb, opt.strict).await?;
		// Process the document record
		let pro = Processed {
			keys_only: false,
			generate: None,
			rid: Some(v.into()),
			ir: None,
			val: Operable::Insert(Value::None.into(), o.into()),
		};
		// Everything ok
		Ok(pro)
	}

	async fn process_key_val(key: Key, val: Val) -> Result<Processed, Error> {
		let key: thing::Thing = (&key).into();
		let val: Value = (&val).into();
		let rid = Thing::from((key.tb, key.id));
		// Create a new operable value
		let val = Operable::Value(val.into());
		// Process the record
		let pro = Processed {
			keys_only: false,
			generate: None,
			rid: Some(rid.into()),
			ir: None,
			val,
		};
		Ok(pro)
	}

	async fn process_index_item(
		opt: &Options,
		txn: &Transaction,
		r: CollectorRecord,
	) -> Result<Processed, Error> {
		let v = if let Some(v) = r.2 {
			// The value may already be fetched by the KNN iterator to evaluate the condition
			v
		} else {
			// Otherwise we have to fetch the record
			Iterable::fetch_thing(txn, opt, &r.0).await?
		};
		let pro = Processed {
			keys_only: false,
			generate: None,
			rid: Some(r.0),
			ir: Some(r.1.into()),
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
		let pro = collected.process(self.opt, self.txn).await?;
		self.ite.process(self.stk, self.ctx, self.opt, self.stm, pro).await?;
		Ok(())
	}
}

pub(super) struct ConcurrentDistinctCollector<'a> {
	coll: ConcurrentCollector<'a>,
	dis: &'a mut SyncDistinct,
}

impl Collector for ConcurrentDistinctCollector<'_> {
	async fn collect(&mut self, collected: Collected) -> Result<(), Error> {
		let pro = collected.process(self.coll.opt, self.coll.txn).await?;
		if !self.dis.check_already_processed(&pro) {
			self.coll
				.ite
				.process(self.coll.stk, self.coll.ctx, self.coll.opt, self.coll.stm, pro)
				.await?;
		}
		Ok(())
	}
}

pub(super) trait Collector {
	async fn collect(&mut self, collected: Collected) -> Result<(), Error>;

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
		if ctx.is_ok() {
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
				Iterable::Range(tb, v, keys_only) => {
					if keys_only {
						self.collect_range_keys(ctx, opt, &tb, v).await?
					} else {
						self.collect_range(ctx, opt, &tb, v).await?
					}
				}
				Iterable::Table(v, keys_only) => {
					let ctx = Self::check_query_planner_context(ctx, &v);
					if keys_only {
						self.collect_table_keys(&ctx, opt, &v).await?
					} else {
						self.collect_table(&ctx, opt, &v).await?
					}
				}
				Iterable::Index(v, irf) => {
					if let Some(qp) = ctx.get_query_planner() {
						if let Some(exe) = qp.get_query_executor(&v.0) {
							// We set the query executor matching the current table in the Context
							// Avoiding search in the hashmap of the query planner for each doc
							let mut ctx = MutableContext::new(ctx);
							ctx.set_query_executor(exe.clone());
							let ctx = ctx.freeze();
							return self.collect_index_items(&ctx, opt, &v, irf).await;
						}
					}
					self.collect_index_items(ctx, opt, &v, irf).await?
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

	async fn collect_table(
		&mut self,
		ctx: &Context,
		opt: &Options,
		v: &Table,
	) -> Result<(), Error> {
		// Get the transaction
		let txn = ctx.tx();
		// Check that the table exists
		txn.check_ns_db_tb(opt.ns()?, opt.db()?, v, opt.strict).await?;
		// Prepare the start and end keys
		let beg = thing::prefix(opt.ns()?, opt.db()?, v);
		let end = thing::suffix(opt.ns()?, opt.db()?, v);
		// Create a new iterable range
		let mut stream = txn.stream(beg..end, opt.version);
		// Loop until no more entries
		while let Some(res) = stream.next().await {
			// Check if the context is finished
			if ctx.is_done() {
				break;
			}
			// Parse the data from the store
			let (k, v) = res?;
			self.collect(Collected::KeyVal(k, v)).await?;
		}
		// Everything ok
		Ok(())
	}

	async fn collect_table_keys(
		&mut self,
		ctx: &Context,
		opt: &Options,
		v: &Table,
	) -> Result<(), Error> {
		// Get the transaction
		let txn = ctx.tx();
		// Check that the table exists
		txn.check_ns_db_tb(opt.ns()?, opt.db()?, v, opt.strict).await?;
		// Prepare the start and end keys
		let beg = thing::prefix(opt.ns()?, opt.db()?, v);
		let end = thing::suffix(opt.ns()?, opt.db()?, v);
		// Create a new iterable range
		let mut stream = txn.stream_keys(beg..end);
		// Loop until no more entries
		while let Some(res) = stream.next().await {
			// Check if the context is finished
			if ctx.is_done() {
				break;
			}
			// Parse the data from the store
			let k = res?;
			// Collect the key
			self.collect(Collected::TableKey(k)).await?;
		}
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
		txn.check_ns_db_tb(opt.ns()?, opt.db()?, tb, opt.strict).await?;
		// Prepare the range start key
		let beg = match &r.beg {
			Bound::Unbounded => thing::prefix(opt.ns()?, opt.db()?, tb),
			Bound::Included(v) => thing::new(opt.ns()?, opt.db()?, tb, v).encode().unwrap(),
			Bound::Excluded(v) => {
				let mut key = thing::new(opt.ns()?, opt.db()?, tb, v).encode().unwrap();
				key.push(0x00);
				key
			}
		};
		// Prepare the range end key
		let end = match &r.end {
			Bound::Unbounded => thing::suffix(opt.ns()?, opt.db()?, tb),
			Bound::Excluded(v) => thing::new(opt.ns()?, opt.db()?, tb, v).encode().unwrap(),
			Bound::Included(v) => {
				let mut key = thing::new(opt.ns()?, opt.db()?, tb, v).encode().unwrap();
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
	) -> Result<(), Error> {
		// Get the transaction
		let txn = ctx.tx();
		// Prepare
		let (beg, end) = Self::range_prepare(&txn, opt, tb, r).await?;
		// Create a new iterable range
		let mut stream = txn.stream(beg..end, None);
		// Loop until no more entries
		while let Some(res) = stream.next().await {
			// Check if the context is finished
			if ctx.is_done() {
				break;
			}
			// Parse the data from the store
			let (k, v) = res?;
			// Collect
			self.collect(Collected::KeyVal(k, v)).await?;
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
	) -> Result<(), Error> {
		// Get the transaction
		let txn = ctx.tx();
		// Prepare
		let (beg, end) = Self::range_prepare(&txn, opt, tb, r).await?;
		// Create a new iterable range
		let mut stream = txn.stream_keys(beg..end);
		// Loop until no more entries
		while let Some(res) = stream.next().await {
			// Check if the context is finished
			if ctx.is_done() {
				break;
			}
			// Parse the data from the store
			let k = res?;
			self.collect(Collected::RangeKey(k)).await?;
		}
		// Everything ok
		Ok(())
	}

	async fn collect_edges(&mut self, ctx: &Context, opt: &Options, e: Edges) -> Result<(), Error> {
		// Pull out options
		let ns = opt.ns()?;
		let db = opt.db()?;
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
				Dir::In => e
					.what
					.iter()
					.map(|v| v.0.to_owned())
					.map(|v| {
						(
							graph::ftprefix(ns, db, tb, id, &e.dir, &v),
							graph::ftsuffix(ns, db, tb, id, &e.dir, &v),
						)
					})
					.collect::<Vec<_>>(),
				// /ns/db/tb/id/OUT/TB
				Dir::Out => e
					.what
					.iter()
					.map(|v| v.0.to_owned())
					.map(|v| {
						(
							graph::ftprefix(ns, db, tb, id, &e.dir, &v),
							graph::ftsuffix(ns, db, tb, id, &e.dir, &v),
						)
					})
					.collect::<Vec<_>>(),
				// /ns/db/tb/id/IN/TB, /ns/db/tb/id/OUT/TB
				Dir::Both => e
					.what
					.iter()
					.map(|v| v.0.to_owned())
					.flat_map(|v| {
						vec![
							(
								graph::ftprefix(ns, db, tb, id, &Dir::In, &v),
								graph::ftsuffix(ns, db, tb, id, &Dir::In, &v),
							),
							(
								graph::ftprefix(ns, db, tb, id, &Dir::Out, &v),
								graph::ftsuffix(ns, db, tb, id, &Dir::Out, &v),
							),
						]
					})
					.collect::<Vec<_>>(),
			},
		};
		// Get the transaction
		let txn = ctx.tx();
		// Check that the table exists
		txn.check_ns_db_tb(opt.ns()?, opt.db()?, tb, opt.strict).await?;
		// Loop over the chosen edge types
		for (beg, end) in keys.into_iter() {
			// Create a new iterable range
			let mut stream = txn.stream(beg..end, None);
			// Loop until no more entries
			while let Some(res) = stream.next().await {
				// Check if the context is finished
				if ctx.is_done() {
					break;
				}
				// Parse the key from the result
				let key = res?.0;
				// Collector the key
				self.collect(Collected::Edge(key)).await?;
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
	) -> Result<(), Error> {
		// Check that the table exists
		ctx.tx().check_ns_db_tb(opt.ns()?, opt.db()?, &table.0, opt.strict).await?;
		if let Some(exe) = ctx.get_query_executor() {
			if let Some(mut iterator) = exe.new_iterator(opt, irf).await? {
				let txn = ctx.tx();
				// Collect by batches
				while !ctx.is_done() {
					let records: Vec<CollectorRecord> =
						iterator.next_batch(ctx, &txn, *NORMAL_FETCH_SIZE).await?;
					if records.is_empty() {
						break;
					}
					for r in records {
						self.collect(Collected::IndexItem(r)).await?;
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
}

impl Iterable {
	/// Returns the value from the store, or Value::None it the value does not exist.
	pub(crate) async fn fetch_thing(
		txn: &Transaction,
		opt: &Options,
		thg: &Thing,
	) -> Result<Arc<Value>, Error> {
		// Fetch and parse the data from the store
		let val = txn.get_record(opt.ns()?, opt.db()?, &thg.tb, &thg.id, None).await?;
		// Return the result
		Ok(val)
	}
}
