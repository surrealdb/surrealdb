use crate::cnf::NORMAL_FETCH_SIZE;
use crate::ctx::{Context, MutableContext};
#[cfg(not(target_arch = "wasm32"))]
use crate::dbs::distinct::AsyncDistinct;
use crate::dbs::distinct::SyncDistinct;
use crate::dbs::{Iterable, Iterator, Operable, Options, Processed, Statement};
use crate::err::Error;
use crate::idx::planner::iterators::{CollectorRecord, IteratorRef, ThingIterator};
use crate::idx::planner::IterationStage;
use crate::key::{graph, thing};
use crate::kvs::Transaction;
use crate::sql::dir::Dir;
use crate::sql::id::range::IdRange;
use crate::sql::{Edges, Table, Thing, Value};
#[cfg(not(target_arch = "wasm32"))]
use channel::Sender;
use futures::StreamExt;
use reblessive::tree::Stk;
use std::borrow::Cow;
use std::ops::Bound;
use std::vec;

impl Iterable {
	pub(crate) async fn iterate(
		self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		ite: &mut Iterator,
		dis: Option<&mut SyncDistinct>,
	) -> Result<(), Error> {
		if self.iteration_stage_check(ctx) {
			Processor::Iterator(dis, ite).process_iterable(stk, ctx, opt, stm, self).await
		} else {
			Ok(())
		}
	}

	#[cfg(not(target_arch = "wasm32"))]
	pub(crate) async fn channel(
		self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		chn: Sender<Processed>,
		dis: Option<AsyncDistinct>,
	) -> Result<(), Error> {
		if self.iteration_stage_check(ctx) {
			Processor::Channel(dis, chn).process_iterable(stk, ctx, opt, stm, self).await
		} else {
			Ok(())
		}
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

pub(crate) enum Processor<'a> {
	Iterator(Option<&'a mut SyncDistinct>, &'a mut Iterator),
	#[cfg(not(target_arch = "wasm32"))]
	Channel(Option<AsyncDistinct>, Sender<Processed>),
}

impl<'a> Processor<'a> {
	async fn process(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		pro: Processed,
	) -> Result<(), Error> {
		match self {
			Processor::Iterator(distinct, ite) => {
				let is_processed = if let Some(d) = distinct {
					d.check_already_processed(&pro)
				} else {
					false
				};
				if !is_processed {
					ite.process(stk, ctx, opt, stm, pro).await;
				}
			}
			#[cfg(not(target_arch = "wasm32"))]
			Processor::Channel(distinct, chn) => {
				let is_processed = if let Some(d) = distinct {
					d.check_already_processed(&pro).await
				} else {
					false
				};
				if !is_processed {
					chn.send(pro).await?;
				}
			}
		};
		Ok(())
	}

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

	async fn process_iterable(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		iterable: Iterable,
	) -> Result<(), Error> {
		if ctx.is_ok() {
			match iterable {
				Iterable::Value(v) => self.process_value(stk, ctx, opt, stm, v).await?,
				Iterable::Yield(v) => self.process_yield(stk, ctx, opt, stm, v).await?,
				Iterable::Thing(v) => self.process_thing(stk, ctx, opt, stm, v).await?,
				Iterable::Defer(v) => self.process_defer(stk, ctx, opt, stm, v).await?,
				Iterable::Edges(e) => self.process_edges(stk, ctx, opt, stm, e).await?,
				Iterable::Range(tb, v, keys_only) => {
					if keys_only {
						self.process_range_keys(stk, ctx, opt, stm, &tb, v).await?
					} else {
						self.process_range(stk, ctx, opt, stm, &tb, v).await?
					}
				}
				Iterable::Table(v, keys_only) => {
					let ctx = Self::check_query_planner_context(ctx, &v);
					if keys_only {
						self.process_table_keys(stk, &ctx, opt, stm, &v).await?
					} else {
						self.process_table(stk, &ctx, opt, stm, &v).await?
					}
				}
				Iterable::Index(t, irf) => {
					if let Some(qp) = ctx.get_query_planner() {
						if let Some(exe) = qp.get_query_executor(&t.0) {
							// We set the query executor matching the current table in the Context
							// Avoiding search in the hashmap of the query planner for each doc
							let mut ctx = MutableContext::new(ctx);
							ctx.set_query_executor(exe.clone());
							let ctx = ctx.freeze();
							return self.process_index(stk, &ctx, opt, stm, &t, irf).await;
						}
					}
					self.process_index(stk, ctx, opt, stm, &t, irf).await?
				}
				Iterable::Mergeable(v, o) => {
					self.process_mergeable(stk, ctx, opt, stm, (v, o)).await?
				}
				Iterable::Relatable(f, v, w, o) => {
					self.process_relatable(stk, ctx, opt, stm, (f, v, w, o)).await?
				}
			}
		}
		Ok(())
	}

	async fn process_value(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		v: Value,
	) -> Result<(), Error> {
		// Pass the value through
		let pro = Processed {
			rid: None,
			ir: None,
			val: Operable::Value(v.into()),
		};
		// Process the document record
		self.process(stk, ctx, opt, stm, pro).await
	}

	async fn process_yield(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		v: Table,
	) -> Result<(), Error> {
		// Fetch the record id if specified
		let v = match stm.data() {
			// There is a data clause so fetch a record id
			Some(data) => match data.rid(stk, ctx, opt).await? {
				// Generate a new id from the id field
				Some(id) => id.generate(&v, false)?,
				// Generate a new random table id
				None => v.generate(),
			},
			// There is no data clause so create a record id
			None => v.generate(),
		};
		// Pass the value through
		let pro = Processed {
			rid: Some(v.into()),
			ir: None,
			val: Operable::Value(Value::None.into()),
		};
		// Process the document record
		self.process(stk, ctx, opt, stm, pro).await
	}

	async fn process_defer(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		v: Thing,
	) -> Result<(), Error> {
		// Check that the table exists
		ctx.tx().check_ns_db_tb(opt.ns()?, opt.db()?, &v.tb, opt.strict).await?;
		// Process the document record
		let pro = Processed {
			rid: Some(v.into()),
			ir: None,
			val: Operable::Value(Value::None.into()),
		};
		self.process(stk, ctx, opt, stm, pro).await?;
		// Everything ok
		Ok(())
	}

	async fn process_thing(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		v: Thing,
	) -> Result<(), Error> {
		// Check that the table exists
		ctx.tx().check_ns_db_tb(opt.ns()?, opt.db()?, &v.tb, opt.strict).await?;
		// Fetch the data from the store
		let key = thing::new(opt.ns()?, opt.db()?, &v.tb, &v.id);
		let val = ctx.tx().get(key, opt.version).await?;
		// Parse the data from the store
		let val = Operable::Value(
			match val {
				Some(v) => Value::from(v),
				None => Value::None,
			}
			.into(),
		);
		// Process the document record
		let pro = Processed {
			rid: Some(v.into()),
			ir: None,
			val,
		};
		self.process(stk, ctx, opt, stm, pro).await?;
		// Everything ok
		Ok(())
	}

	async fn process_mergeable(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		(v, o): (Thing, Value),
	) -> Result<(), Error> {
		// Check that the table exists
		ctx.tx().check_ns_db_tb(opt.ns()?, opt.db()?, &v.tb, opt.strict).await?;
		// Process the document record
		let pro = Processed {
			rid: Some(v.into()),
			ir: None,
			val: Operable::Mergeable(Value::None.into(), o.into(), false),
		};
		self.process(stk, ctx, opt, stm, pro).await?;
		// Everything ok
		Ok(())
	}

	async fn process_relatable(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		(f, v, w, o): (Thing, Thing, Thing, Option<Value>),
	) -> Result<(), Error> {
		// Check that the table exists
		ctx.tx().check_ns_db_tb(opt.ns()?, opt.db()?, &v.tb, opt.strict).await?;
		// Fetch the data from the store
		let key = thing::new(opt.ns()?, opt.db()?, &v.tb, &v.id);
		let val = ctx.tx().get(key, None).await?;
		// Parse the data from the store
		let x = match val {
			Some(v) => Value::from(v),
			None => Value::None,
		};
		// Create a new operable value
		let val = Operable::Relatable(f, x.into(), w, o.map(|v| v.into()), false);
		// Process the document record
		let pro = Processed {
			rid: Some(v.into()),
			ir: None,
			val,
		};
		self.process(stk, ctx, opt, stm, pro).await?;
		// Everything ok
		Ok(())
	}

	async fn process_table(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
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
			let key: thing::Thing = (&k).into();
			let val: Value = (&v).into();
			let rid = Thing::from((key.tb, key.id));
			// Create a new operable value
			let val = Operable::Value(val.into());
			// Process the record
			let pro = Processed {
				rid: Some(rid.into()),
				ir: None,
				val,
			};
			self.process(stk, ctx, opt, stm, pro).await?;
		}
		// Everything ok
		Ok(())
	}

	async fn process_table_keys(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
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
			let key: thing::Thing = (&k).into();
			let rid = Thing::from((key.tb, key.id));
			// Process the record
			let pro = Processed {
				rid: Some(rid.into()),
				ir: None,
				val: Operable::Value(Value::Null.into()),
			};
			self.process(stk, ctx, opt, stm, pro).await?;
		}
		// Everything ok
		Ok(())
	}

	async fn process_range_prepare(
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

	async fn process_range(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		tb: &str,
		r: IdRange,
	) -> Result<(), Error> {
		// Get the transaction
		let txn = ctx.tx();
		// Prepare
		let (beg, end) = Self::process_range_prepare(&txn, opt, tb, r).await?;
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
			let key: thing::Thing = (&k).into();
			let val: Value = (&v).into();
			let rid = Thing::from((key.tb, key.id));
			// Create a new operable value
			let val = Operable::Value(val.into());
			// Process the record
			let pro = Processed {
				rid: Some(rid.into()),
				ir: None,
				val,
			};
			self.process(stk, ctx, opt, stm, pro).await?;
		}
		// Everything ok
		Ok(())
	}

	async fn process_range_keys(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		tb: &str,
		r: IdRange,
	) -> Result<(), Error> {
		// Get the transaction
		let txn = ctx.tx();
		// Prepare
		let (beg, end) = Self::process_range_prepare(&txn, opt, tb, r).await?;
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
			let key: thing::Thing = (&k).into();
			let val = Value::Null;
			let rid = Thing::from((key.tb, key.id));
			// Create a new operable value
			let val = Operable::Value(val.into());
			// Process the record
			let pro = Processed {
				rid: Some(rid.into()),
				ir: None,
				val,
			};
			self.process(stk, ctx, opt, stm, pro).await?;
		}
		// Everything ok
		Ok(())
	}

	async fn process_edges(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		e: Edges,
	) -> Result<(), Error> {
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
				// Parse the data from the store
				let gra: graph::Graph = graph::Graph::decode(&key)?;
				// Fetch the data from the store
				let key = thing::new(opt.ns()?, opt.db()?, gra.ft, &gra.fk);
				let val = txn.get(key, None).await?;
				let rid = Thing::from((gra.ft, gra.fk));
				// Parse the data from the store
				let val = Operable::Value(match val {
					Some(v) => Value::from(v).into(),
					None => Value::None.into(),
				});
				// Process the record
				let pro = Processed {
					rid: Some(rid.into()),
					ir: None,
					val,
				};
				self.process(stk, ctx, opt, stm, pro).await?;
			}
		}
		// Everything ok
		Ok(())
	}

	async fn process_index(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		table: &Table,
		irf: IteratorRef,
	) -> Result<(), Error> {
		// Check that the table exists
		ctx.tx().check_ns_db_tb(opt.ns()?, opt.db()?, &table.0, opt.strict).await?;
		if let Some(exe) = ctx.get_query_executor() {
			if let Some(mut iterator) = exe.new_iterator(opt, irf).await? {
				// Get the first batch
				let mut to_process = Self::next_batch(ctx, opt, &mut iterator).await?;

				while !to_process.is_empty() {
					// Check if the context is finished
					if ctx.is_done() {
						break;
					}
					// Process the records
					// TODO: par_iter
					for pro in to_process {
						self.process(stk, ctx, opt, stm, pro).await?;
					}
					// Get the next batch
					to_process = Self::next_batch(ctx, opt, &mut iterator).await?;
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

	async fn next_batch(
		ctx: &Context,
		opt: &Options,
		iterator: &mut ThingIterator,
	) -> Result<Vec<Processed>, Error> {
		let txn = ctx.tx();
		let records: Vec<CollectorRecord> =
			iterator.next_batch(ctx, &txn, *NORMAL_FETCH_SIZE).await?;
		let mut to_process = Vec::with_capacity(records.len());
		for r in records {
			let v = if let Some(v) = r.2 {
				// The value may be already be fetched by the KNN iterator to evaluate the condition
				v
			} else {
				// Otherwise we have to fetch the record
				Iterable::fetch_thing(&txn, opt, &r.0).await?.into()
			};
			let p = Processed {
				rid: Some(r.0),
				ir: Some(r.1.into()),
				val: Operable::Value(v),
			};
			to_process.push(p);
		}
		Ok(to_process)
	}
}

impl Iterable {
	/// Returns the value from the store, or Value::None it the value does not exist.
	pub(crate) async fn fetch_thing(
		txn: &Transaction,
		opt: &Options,
		thg: &Thing,
	) -> Result<Value, Error> {
		// Fetch the data from the store
		let key = thing::new(opt.ns()?, opt.db()?, &thg.tb, &thg.id);
		// Fetch and parse the data from the store
		let val = txn.get(key, None).await?.map(Value::from).unwrap_or(Value::None);
		// Return the result
		Ok(val)
	}
}
