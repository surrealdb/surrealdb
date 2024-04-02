use crate::cnf::PROCESSOR_BATCH_SIZE;
use crate::ctx::Context;
#[cfg(not(target_arch = "wasm32"))]
use crate::dbs::distinct::AsyncDistinct;
use crate::dbs::distinct::SyncDistinct;
use crate::dbs::{Iterable, Iterator, Operable, Options, Processed, Statement, Transaction};
use crate::err::Error;
use crate::idx::planner::executor::IteratorRef;
use crate::idx::planner::IterationStage;
use crate::key::{graph, thing};
use crate::kvs::ScanPage;
use crate::sql::dir::Dir;
use crate::sql::{Edges, Range, Table, Thing, Value};
#[cfg(not(target_arch = "wasm32"))]
use channel::Sender;
use std::ops::Bound;

impl Iterable {
	pub(crate) async fn iterate(
		self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		ite: &mut Iterator,
		dis: Option<&mut SyncDistinct>,
	) -> Result<(), Error> {
		if self.iteration_stage_check(ctx) {
			Processor::Iterator(dis, ite).process_iterable(ctx, opt, txn, stm, self).await
		} else {
			Ok(())
		}
	}

	#[cfg(not(target_arch = "wasm32"))]
	pub(crate) async fn channel(
		self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		chn: Sender<Processed>,
		dis: Option<AsyncDistinct>,
	) -> Result<(), Error> {
		if self.iteration_stage_check(ctx) {
			Processor::Channel(dis, chn).process_iterable(ctx, opt, txn, stm, self).await
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
							return exe.has_knn();
						}
					}
				}
			}
			_ => {}
		}
		true
	}
}

enum Processor<'a> {
	Iterator(Option<&'a mut SyncDistinct>, &'a mut Iterator),
	#[cfg(not(target_arch = "wasm32"))]
	Channel(Option<AsyncDistinct>, Sender<Processed>),
}

impl<'a> Processor<'a> {
	async fn process(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
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
					ite.process(ctx, opt, txn, stm, pro).await;
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
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		iterable: Iterable,
	) -> Result<(), Error> {
		if ctx.is_ok() {
			match iterable {
				Iterable::Value(v) => self.process_value(ctx, opt, txn, stm, v).await?,
				Iterable::Thing(v) => self.process_thing(ctx, opt, txn, stm, v).await?,
				Iterable::Defer(v) => self.process_defer(ctx, opt, txn, stm, v).await?,
				Iterable::Table(v) => {
					if let Some(qp) = ctx.get_query_planner() {
						if let Some(exe) = qp.get_query_executor(&v.0) {
							// We set the query executor matching the current table in the Context
							// Avoiding search in the hashmap of the query planner for each doc
							let mut ctx = Context::new(ctx);
							ctx.set_query_executor(exe.clone());
							return self.process_table(&ctx, opt, txn, stm, v.as_ref()).await;
						}
					}
					self.process_table(ctx, opt, txn, stm, v.as_ref()).await?
				}
				Iterable::Range(v) => self.process_range(ctx, opt, txn, stm, v).await?,
				Iterable::Edges(e) => self.process_edge(ctx, opt, txn, stm, e).await?,
				Iterable::Index(t, ir) => {
					if let Some(qp) = ctx.get_query_planner() {
						if let Some(exe) = qp.get_query_executor(&t.0) {
							// We set the query executor matching the current table in the Context
							// Avoiding search in the hashmap of the query planner for each doc
							let mut ctx = Context::new(ctx);
							ctx.set_query_executor(exe.clone());
							return self.process_index(&ctx, opt, txn, stm, t.as_ref(), ir).await;
						}
					}
					self.process_index(ctx, opt, txn, stm, t.as_ref(), ir).await?
				}
				Iterable::Mergeable(v, o) => {
					self.process_mergeable(ctx, opt, txn, stm, v, o).await?
				}
				Iterable::Relatable(f, v, w) => {
					self.process_relatable(ctx, opt, txn, stm, f, v, w).await?
				}
			}
		}
		Ok(())
	}

	async fn process_value(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		v: Value,
	) -> Result<(), Error> {
		// Pass the value through
		let pro = Processed {
			ir: None,
			rid: None,
			doc_id: None,
			val: Operable::Value(v),
		};
		// Process the document record
		self.process(ctx, opt, txn, stm, pro).await
	}

	async fn process_thing(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		v: Thing,
	) -> Result<(), Error> {
		// Check that the table exists
		txn.lock().await.check_ns_db_tb(opt.ns(), opt.db(), &v.tb, opt.strict).await?;
		// Fetch the data from the store
		let key = thing::new(opt.ns(), opt.db(), &v.tb, &v.id);
		let val = txn.clone().lock().await.get(key).await?;
		// Parse the data from the store
		let val = Operable::Value(match val {
			Some(v) => Value::from(v),
			None => Value::None,
		});
		// Process the document record
		let pro = Processed {
			ir: None,
			rid: Some(v),
			doc_id: None,
			val,
		};
		self.process(ctx, opt, txn, stm, pro).await?;
		// Everything ok
		Ok(())
	}

	async fn process_defer(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		v: Thing,
	) -> Result<(), Error> {
		// Check that the table exists
		txn.lock().await.check_ns_db_tb(opt.ns(), opt.db(), &v.tb, opt.strict).await?;
		// Process the document record
		let pro = Processed {
			ir: None,
			rid: Some(v),
			doc_id: None,
			val: Operable::Value(Value::None),
		};
		self.process(ctx, opt, txn, stm, pro).await?;
		// Everything ok
		Ok(())
	}

	async fn process_mergeable(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		v: Thing,
		o: Value,
	) -> Result<(), Error> {
		// Check that the table exists
		txn.lock().await.check_ns_db_tb(opt.ns(), opt.db(), &v.tb, opt.strict).await?;
		// Fetch the data from the store
		let key = thing::new(opt.ns(), opt.db(), &v.tb, &v.id);
		let val = txn.clone().lock().await.get(key).await?;
		// Parse the data from the store
		let x = match val {
			Some(v) => Value::from(v),
			None => Value::None,
		};
		// Create a new operable value
		let val = Operable::Mergeable(x, o);
		// Process the document record
		let pro = Processed {
			ir: None,
			rid: Some(v),
			doc_id: None,
			val,
		};
		self.process(ctx, opt, txn, stm, pro).await?;
		// Everything ok
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn process_relatable(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		f: Thing,
		v: Thing,
		w: Thing,
	) -> Result<(), Error> {
		// Check that the table exists
		txn.lock().await.check_ns_db_tb(opt.ns(), opt.db(), &v.tb, opt.strict).await?;
		// Fetch the data from the store
		let key = thing::new(opt.ns(), opt.db(), &v.tb, &v.id);
		let val = txn.clone().lock().await.get(key).await?;
		// Parse the data from the store
		let x = match val {
			Some(v) => Value::from(v),
			None => Value::None,
		};
		// Create a new operable value
		let val = Operable::Relatable(f, x, w);
		// Process the document record
		let pro = Processed {
			ir: None,
			rid: Some(v),
			doc_id: None,
			val,
		};
		self.process(ctx, opt, txn, stm, pro).await?;
		// Everything ok
		Ok(())
	}

	async fn process_table(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		v: &Table,
	) -> Result<(), Error> {
		// Check that the table exists
		txn.lock().await.check_ns_db_tb(opt.ns(), opt.db(), v, opt.strict).await?;
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
			let res = txn.clone().lock().await.scan_paged(page, PROCESSOR_BATCH_SIZE).await?;
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
					ir: None,
					rid: Some(rid),
					doc_id: None,
					val,
				};
				self.process(ctx, opt, txn, stm, pro).await?;
			}
			continue;
		}
		// Everything ok
		Ok(())
	}

	async fn process_range(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		v: Range,
	) -> Result<(), Error> {
		// Check that the table exists
		txn.lock().await.check_ns_db_tb(opt.ns(), opt.db(), &v.tb, opt.strict).await?;
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
			let res = txn.clone().lock().await.scan_paged(page, PROCESSOR_BATCH_SIZE).await?;
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
					ir: None,
					rid: Some(rid),
					doc_id: None,
					val,
				};
				self.process(ctx, opt, txn, stm, pro).await?;
			}
			continue;
		}
		// Everything ok
		Ok(())
	}

	async fn process_edge(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
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
				let res = txn.lock().await.scan_paged(page, PROCESSOR_BATCH_SIZE).await?;
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
					let val = txn.lock().await.get(key).await?;
					let rid = Thing::from((gra.ft, gra.fk));
					// Parse the data from the store
					let val = Operable::Value(match val {
						Some(v) => Value::from(v),
						None => Value::None,
					});
					// Process the record
					let pro = Processed {
						ir: None,
						rid: Some(rid),
						doc_id: None,
						val,
					};
					self.process(ctx, opt, txn, stm, pro).await?;
				}
				continue;
			}
		}
		// Everything ok
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn process_index(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		table: &Table,
		ir: IteratorRef,
	) -> Result<(), Error> {
		// Check that the table exists
		txn.lock().await.check_ns_db_tb(opt.ns(), opt.db(), &table.0, opt.strict).await?;
		if let Some(exe) = ctx.get_query_executor() {
			if let Some(mut iterator) = exe.new_iterator(opt, ir).await? {
				let mut things = iterator.next_batch(txn, PROCESSOR_BATCH_SIZE).await?;
				while !things.is_empty() {
					// Check if the context is finished
					if ctx.is_done() {
						break;
					}

					for (thing, doc_id) in things {
						// Check the context
						if ctx.is_done() {
							break;
						}

						// If the record is from another table we can skip
						if !thing.tb.eq(table.as_str()) {
							continue;
						}

						// Fetch the data from the store
						let key = thing::new(opt.ns(), opt.db(), &table.0, &thing.id);
						let val = txn.lock().await.get(key.clone()).await?;
						let rid = Thing::from((key.tb, key.id));
						// Parse the data from the store
						let val = Operable::Value(match val {
							Some(v) => Value::from(v),
							None => Value::None,
						});
						// Process the document record
						let pro = Processed {
							ir: Some(ir),
							rid: Some(rid),
							doc_id,
							val,
						};
						self.process(ctx, opt, txn, stm, pro).await?;
					}

					// Collect the next batch of ids
					things = iterator.next_batch(txn, PROCESSOR_BATCH_SIZE).await?;
				}
				// Everything ok
				return Ok(());
			} else {
				return Err(Error::QueryNotExecutedDetail {
					message: "No Iterator has been found.".to_string(),
				});
			}
		}
		Err(Error::QueryNotExecutedDetail {
			message: "No QueryExecutor has been found.".to_string(),
		})
	}
}
