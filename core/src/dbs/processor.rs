use crate::cnf::PROCESSOR_BATCH_SIZE;
use crate::ctx::Context;
#[cfg(not(target_arch = "wasm32"))]
use crate::dbs::distinct::AsyncDistinct;
use crate::dbs::distinct::SyncDistinct;
use crate::dbs::{Iterable, Iterator, Operable, Options, Processed, Statement};
use crate::err::Error;
use crate::idx::planner::iterators::{CollectorRecord, IteratorRef, ThingIterator};
use crate::idx::planner::IterationStage;
use crate::key::{graph, thing};
use crate::kvs;
use crate::kvs::ScanPage;
use crate::sql::dir::Dir;
use crate::sql::{Edges, Range, Table, Thing, Value};
#[cfg(not(target_arch = "wasm32"))]
use channel::Sender;
use reblessive::tree::Stk;
use std::ops::Bound;
use std::vec;

impl Iterable {
	pub(crate) async fn iterate(
		self,
		stk: &mut Stk,
		ctx: &Context<'_>,
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
		ctx: &Context<'_>,
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

	fn iteration_stage_check(&self, ctx: &Context<'_>) -> bool {
		match self {
			Iterable::Table(tb) | Iterable::Index(tb, _) => {
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
		ctx: &Context<'_>,
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

	async fn process_iterable(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		iterable: Iterable,
	) -> Result<(), Error> {
		if ctx.is_ok() {
			match iterable {
				Iterable::Value(v) => self.process_value(stk, ctx, opt, stm, v).await?,
				Iterable::Thing(v) => self.process_thing(stk, ctx, opt, stm, v).await?,
				Iterable::Defer(v) => self.process_defer(stk, ctx, opt, stm, v).await?,
				Iterable::Table(v) => {
					if let Some(qp) = ctx.get_query_planner() {
						if let Some(exe) = qp.get_query_executor(&v.0) {
							// We set the query executor matching the current table in the Context
							// Avoiding search in the hashmap of the query planner for each doc
							let mut ctx = Context::new(ctx);
							ctx.set_query_executor(exe.clone());
							return self.process_table(stk, &ctx, opt, stm, &v).await;
						}
					}
					self.process_table(stk, ctx, opt, stm, &v).await?
				}
				Iterable::Range(v) => self.process_range(stk, ctx, opt, stm, v).await?,
				Iterable::Edges(e) => self.process_edge(stk, ctx, opt, stm, e).await?,
				Iterable::Index(t, irf) => {
					if let Some(qp) = ctx.get_query_planner() {
						if let Some(exe) = qp.get_query_executor(&t.0) {
							// We set the query executor matching the current table in the Context
							// Avoiding search in the hashmap of the query planner for each doc
							let mut ctx = Context::new(ctx);
							ctx.set_query_executor(exe.clone());
							return self.process_index(stk, &ctx, opt, stm, &t, irf).await;
						}
					}
					self.process_index(stk, ctx, opt, stm, &t, irf).await?
				}
				Iterable::Mergeable(v, o) => {
					self.process_mergeable(stk, ctx, opt, stm, v, o).await?
				}
				Iterable::Relatable(f, v, w, o) => {
					self.process_relatable(stk, ctx, opt, stm, f, v, w, o).await?
				}
			}
		}
		Ok(())
	}

	async fn process_value(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		v: Value,
	) -> Result<(), Error> {
		// Pass the value through
		let pro = Processed {
			rid: None,
			ir: None,
			val: Operable::Value(v),
		};
		// Process the document record
		self.process(stk, ctx, opt, stm, pro).await
	}

	async fn process_thing(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		v: Thing,
	) -> Result<(), Error> {
		// Check that the table exists
		ctx.tx_lock().await.check_ns_db_tb(opt.ns(), opt.db(), &v.tb, opt.strict).await?;
		// Fetch the data from the store
		let key = thing::new(opt.ns(), opt.db(), &v.tb, &v.id);
		let val = ctx.tx_lock().await.get(key).await?;
		// Parse the data from the store
		let val = Operable::Value(match val {
			Some(v) => Value::from(v),
			None => Value::None,
		});
		// Process the document record
		let pro = Processed {
			rid: Some(v),
			ir: None,
			val,
		};
		self.process(stk, ctx, opt, stm, pro).await?;
		// Everything ok
		Ok(())
	}

	async fn process_defer(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		v: Thing,
	) -> Result<(), Error> {
		// Check that the table exists
		ctx.tx_lock().await.check_ns_db_tb(opt.ns(), opt.db(), &v.tb, opt.strict).await?;
		// Process the document record
		let pro = Processed {
			rid: Some(v),
			ir: None,
			val: Operable::Value(Value::None),
		};
		self.process(stk, ctx, opt, stm, pro).await?;
		// Everything ok
		Ok(())
	}

	async fn process_mergeable(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		v: Thing,
		o: Value,
	) -> Result<(), Error> {
		// Check that the table exists
		ctx.tx_lock().await.check_ns_db_tb(opt.ns(), opt.db(), &v.tb, opt.strict).await?;
		// Fetch the data from the store
		let key = thing::new(opt.ns(), opt.db(), &v.tb, &v.id);
		let val = ctx.tx_lock().await.get(key).await?;
		// Parse the data from the store
		let x = match val {
			Some(v) => Value::from(v),
			None => Value::None,
		};
		// Create a new operable value
		let val = Operable::Mergeable(x, o);
		// Process the document record
		let pro = Processed {
			rid: Some(v),
			ir: None,
			val,
		};
		self.process(stk, ctx, opt, stm, pro).await?;
		// Everything ok
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn process_relatable(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		f: Thing,
		v: Thing,
		w: Thing,
		o: Option<Value>,
	) -> Result<(), Error> {
		// Check that the table exists
		ctx.tx_lock().await.check_ns_db_tb(opt.ns(), opt.db(), &v.tb, opt.strict).await?;
		// Fetch the data from the store
		let key = thing::new(opt.ns(), opt.db(), &v.tb, &v.id);
		let val = ctx.tx_lock().await.get(key).await?;
		// Parse the data from the store
		let x = match val {
			Some(v) => Value::from(v),
			None => Value::None,
		};
		// Create a new operable value
		let val = Operable::Relatable(f, x, w, o);
		// Process the document record
		let pro = Processed {
			rid: Some(v),
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
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		v: &Table,
	) -> Result<(), Error> {
		// Check that the table exists
		ctx.tx_lock().await.check_ns_db_tb(opt.ns(), opt.db(), v, opt.strict).await?;
		// Prepare the start and end keys
		let beg = thing::prefix(opt.ns(), opt.db(), v);
		let end = thing::suffix(opt.ns(), opt.db(), v);
		// Loop until no more keys
		let mut next_page = Some(ScanPage::from(beg..end));
		while let Some(page) = next_page {
			// Check if the context is finished
			if ctx.is_done() {
				break;
			}
			// Get the next batch of key-value entries
			let res = ctx.tx_lock().await.scan_paged(page, PROCESSOR_BATCH_SIZE).await?;
			next_page = res.next_page;
			let res = res.values;
			// If no results then break
			if res.is_empty() {
				break;
			}
			// Loop over results
			for (k, v) in res.into_iter() {
				// Check the context
				if ctx.is_done() {
					break;
				}
				// Parse the data from the store
				let key: thing::Thing = (&k).into();
				let val: Value = (&v).into();
				let rid = Thing::from((key.tb, key.id));
				// Create a new operable value
				let val = Operable::Value(val);
				// Process the record
				let pro = Processed {
					rid: Some(rid),
					ir: None,
					val,
				};
				self.process(stk, ctx, opt, stm, pro).await?;
			}
			continue;
		}
		// Everything ok
		Ok(())
	}

	async fn process_range(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		v: Range,
	) -> Result<(), Error> {
		// Check that the table exists
		ctx.tx_lock().await.check_ns_db_tb(opt.ns(), opt.db(), &v.tb, opt.strict).await?;
		// Prepare the range start key
		let beg = match &v.beg {
			Bound::Unbounded => thing::prefix(opt.ns(), opt.db(), &v.tb),
			Bound::Included(id) => thing::new(opt.ns(), opt.db(), &v.tb, id).encode().unwrap(),
			Bound::Excluded(id) => {
				let mut key = thing::new(opt.ns(), opt.db(), &v.tb, id).encode().unwrap();
				key.push(0x00);
				key
			}
		};
		// Prepare the range end key
		let end = match &v.end {
			Bound::Unbounded => thing::suffix(opt.ns(), opt.db(), &v.tb),
			Bound::Excluded(id) => thing::new(opt.ns(), opt.db(), &v.tb, id).encode().unwrap(),
			Bound::Included(id) => {
				let mut key = thing::new(opt.ns(), opt.db(), &v.tb, id).encode().unwrap();
				key.push(0x00);
				key
			}
		};
		// Loop until no more keys
		let mut next_page = Some(ScanPage::from(beg..end));
		while let Some(page) = next_page {
			// Check if the context is finished
			if ctx.is_done() {
				break;
			}
			let res = ctx.tx_lock().await.scan_paged(page, PROCESSOR_BATCH_SIZE).await?;
			next_page = res.next_page;
			// Get the next batch of key-value entries
			let res = res.values;
			// If there are key-value entries then fetch them
			if res.is_empty() {
				break;
			}
			// Loop over results
			for (k, v) in res.into_iter() {
				// Check the context
				if ctx.is_done() {
					break;
				}
				// Parse the data from the store
				let key: thing::Thing = (&k).into();
				let val: Value = (&v).into();
				let rid = Thing::from((key.tb, key.id));
				// Create a new operable value
				let val = Operable::Value(val);
				// Process the record
				let pro = Processed {
					rid: Some(rid),
					ir: None,
					val,
				};
				self.process(stk, ctx, opt, stm, pro).await?;
			}
			continue;
		}
		// Everything ok
		Ok(())
	}

	async fn process_edge(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		e: Edges,
	) -> Result<(), Error> {
		// Pull out options
		let ns = opt.ns();
		let db = opt.db();
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
					.map(|v| v.to_string())
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
					.map(|v| v.to_string())
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
					.map(|v| v.to_string())
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
		//
		for (beg, end) in keys.iter() {
			// Loop until no more keys
			let mut next_page = Some(ScanPage::from(beg.clone()..end.clone()));
			while let Some(page) = next_page {
				// Check if the context is finished
				if ctx.is_done() {
					break;
				}
				// Get the next batch key-value entries
				let res = ctx.tx_lock().await.scan_paged(page, PROCESSOR_BATCH_SIZE).await?;
				next_page = res.next_page;
				let res = res.values;
				// If there are key-value entries then fetch them
				if res.is_empty() {
					break;
				}
				// Loop over results
				for (k, _) in res.into_iter() {
					// Check the context
					if ctx.is_done() {
						break;
					}
					// Parse the data from the store
					let gra: graph::Graph = graph::Graph::decode(&k)?;
					// Fetch the data from the store
					let key = thing::new(opt.ns(), opt.db(), gra.ft, &gra.fk);
					let val = ctx.tx_lock().await.get(key).await?;
					let rid = Thing::from((gra.ft, gra.fk));
					// Parse the data from the store
					let val = Operable::Value(match val {
						Some(v) => Value::from(v),
						None => Value::None,
					});
					// Process the record
					let pro = Processed {
						rid: Some(rid),
						ir: None,
						val,
					};
					self.process(stk, ctx, opt, stm, pro).await?;
				}
				continue;
			}
		}
		// Everything ok
		Ok(())
	}

	async fn process_index(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		table: &Table,
		irf: IteratorRef,
	) -> Result<(), Error> {
		// Check that the table exists
		ctx.tx_lock().await.check_ns_db_tb(opt.ns(), opt.db(), &table.0, opt.strict).await?;
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
		ctx: &Context<'_>,
		opt: &Options,
		iterator: &mut ThingIterator,
	) -> Result<Vec<Processed>, Error> {
		let mut tx = ctx.tx_lock().await;
		let records: Vec<CollectorRecord> =
			iterator.next_batch(ctx, &mut tx, PROCESSOR_BATCH_SIZE).await?;
		let mut to_process = Vec::with_capacity(records.len());
		for r in records {
			let v = if let Some(v) = r.2 {
				// The value may be already be fetched by the KNN iterator to evaluate the condition
				v
			} else {
				// Otherwise we have to fetch the record
				Iterable::fetch_thing(&mut tx, opt, &r.0).await?
			};
			let p = Processed {
				rid: Some(r.0),
				ir: Some(r.1),
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
		tx: &mut kvs::Transaction,
		opt: &Options,
		thg: &Thing,
	) -> Result<Value, Error> {
		// Fetch the data from the store
		let key = thing::new(opt.ns(), opt.db(), &thg.tb, &thg.id);
		// Fetch and parse the data from the store
		let val = tx.get(key).await?.map(Value::from).unwrap_or(Value::None);
		// Return the result
		Ok(val)
	}
}
