use crate::ctx::Context;
use crate::dbs::Iterable;
use crate::dbs::Iterator;
use crate::dbs::Operable;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::err::Error;
use crate::idx::planner::plan::Plan;
use crate::key::graph;
use crate::key::thing;
use crate::sql::dir::Dir;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use crate::sql::{Edges, Range, Table};
use std::ops::Bound;

impl Iterable {
	pub(crate) async fn iterate(
		self,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		ite: &mut Iterator,
	) -> Result<(), Error> {
		if ctx.is_ok() {
			match self {
				Iterable::Value(v) => Self::iterate_value(ctx, opt, stm, v, ite).await,
				Iterable::Thing(v) => Self::iterate_thing(ctx, opt, stm, v, ite).await?,
				Iterable::Mergeable(v, o) => {
					Self::iterate_mergeable(ctx, opt, stm, v, o, ite).await?;
				}
				Iterable::Relatable(f, v, w) => {
					Self::iterate_relatable(ctx, opt, stm, f, v, w, ite).await?
				}
				Iterable::Table(v) => Self::iterate_table(ctx, opt, stm, v, ite).await?,
				Iterable::Range(v) => Self::iterate_range(ctx, opt, stm, v, ite).await?,
				Iterable::Edges(e) => Self::iterate_edge(ctx, opt, stm, e, ite).await?,
				Iterable::Index(t, p) => {
					Self::iterate_index(ctx, opt, stm, t, p, ite).await?;
				}
			}
		}
		Ok(())
	}

	async fn iterate_value(
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		v: Value,
		ite: &mut Iterator,
	) {
		// Pass the value through
		let val = Operable::Value(v);
		// Process the document record
		ite.process(ctx, opt, stm, val).await;
	}

	async fn iterate_thing(
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		v: Thing,
		ite: &mut Iterator,
	) -> Result<(), Error> {
		// Clone transaction
		let txn = ctx.clone_transaction()?;
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
		// Get the optional query executor
		let mut child_ctx = Context::new(ctx);
		child_ctx.add_thing(&v);
		// Process the document record
		ite.process(&child_ctx, opt, stm, val).await;
		Ok(())
	}

	async fn iterate_mergeable(
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		v: Thing,
		o: Value,
		ite: &mut Iterator,
	) -> Result<(), Error> {
		// Clone transaction
		let txn = ctx.clone_transaction()?;
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
		// Create a new context to process the operable
		let mut child_ctx = Context::new(ctx);
		child_ctx.add_thing(&v);
		// Process the document record
		ite.process(&child_ctx, opt, stm, val).await;
		Ok(())
	}

	async fn iterate_relatable(
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		f: Thing,
		v: Thing,
		w: Thing,
		ite: &mut Iterator,
	) -> Result<(), Error> {
		// Clone transaction
		let txn = ctx.clone_transaction()?;
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
		// Create the child context
		let mut child_ctx = Context::new(ctx);
		child_ctx.add_thing(&v);
		// Process the document record
		ite.process(&child_ctx, opt, stm, val).await;
		Ok(())
	}

	async fn iterate_table(
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		v: Table,
		ite: &mut Iterator,
	) -> Result<(), Error> {
		// Clone transaction
		let txn = ctx.clone_transaction()?;
		// Check that the table exists
		txn.lock().await.check_ns_db_tb(opt.ns(), opt.db(), &v, opt.strict).await?;
		// Prepare the start and end keys
		let beg = thing::prefix(opt.ns(), opt.db(), &v);
		let end = thing::suffix(opt.ns(), opt.db(), &v);
		// Prepare the next holder key
		let mut nxt: Option<Vec<u8>> = None;
		// Loop until no more keys
		loop {
			// Check if the context is finished
			if ctx.is_done() {
				break;
			}
			// Get the next 1000 key-value entries
			let res = match nxt {
				None => {
					let min = beg.clone();
					let max = end.clone();
					txn.clone().lock().await.scan(min..max, 1000).await?
				}
				Some(ref mut beg) => {
					beg.push(0x00);
					let min = beg.clone();
					let max = end.clone();
					txn.clone().lock().await.scan(min..max, 1000).await?
				}
			};
			// If there are key-value entries then fetch them
			if !res.is_empty() {
				// Get total results
				let n = res.len();
				// Loop over results
				for (i, (k, v)) in res.into_iter().enumerate() {
					// Check the context
					if ctx.is_done() {
						break;
					}
					// Ready the next
					if n == i + 1 {
						nxt = Some(k.clone());
					}
					// Parse the data from the store
					let key: crate::key::thing::Thing = (&k).into();
					let val: crate::sql::value::Value = (&v).into();
					let rid = Thing::from((key.tb, key.id));
					// Create a new operable value
					let val = Operable::Value(val);
					let mut child_ctx = Context::new(&ctx);
					child_ctx.add_thing(&rid);
					// Process the record
					ite.process(&child_ctx, opt, stm, val).await;
				}
				continue;
			}
			break;
		}
		Ok(())
	}

	async fn iterate_range(
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		v: Range,
		ite: &mut Iterator,
	) -> Result<(), Error> {
		// Clone transaction
		let txn = ctx.clone_transaction()?;
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
		// Prepare the next holder key
		let mut nxt: Option<Vec<u8>> = None;
		// Loop until no more keys
		loop {
			// Check if the context is finished
			if ctx.is_done() {
				break;
			}
			// Get the next 1000 key-value entries
			let res = match nxt {
				None => {
					let min = beg.clone();
					let max = end.clone();
					txn.clone().lock().await.scan(min..max, 1000).await?
				}
				Some(ref mut beg) => {
					beg.push(0x00);
					let min = beg.clone();
					let max = end.clone();
					txn.clone().lock().await.scan(min..max, 1000).await?
				}
			};
			// If there are key-value entries then fetch them
			if !res.is_empty() {
				// Get total results
				let n = res.len();
				// Loop over results
				for (i, (k, v)) in res.into_iter().enumerate() {
					// Check the context
					if ctx.is_done() {
						break;
					}
					// Ready the next
					if n == i + 1 {
						nxt = Some(k.clone());
					}
					// Parse the data from the store
					let key: crate::key::thing::Thing = (&k).into();
					let val: crate::sql::value::Value = (&v).into();
					let rid = Thing::from((key.tb, key.id));
					let mut ctx = Context::new(ctx);
					ctx.add_thing(&rid);
					// Create a new operable value
					let val = Operable::Value(val);
					// Process the record
					ite.process(&ctx, opt, stm, val).await;
				}
				continue;
			}
			break;
		}
		Ok(())
	}

	async fn iterate_edge(
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		e: Edges,
		ite: &mut Iterator,
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
			// Prepare the next holder key
			let mut nxt: Option<Vec<u8>> = None;
			// Loop until no more keys
			loop {
				// Check if the context is finished
				if ctx.is_done() {
					break;
				}
				// Get the next 1000 key-value entries
				let res = match nxt {
					None => {
						let min = beg.clone();
						let max = end.clone();
						ctx.clone_transaction()?.lock().await.scan(min..max, 1000).await?
					}
					Some(ref mut beg) => {
						beg.push(0x00);
						let min = beg.clone();
						let max = end.clone();
						ctx.clone_transaction()?.lock().await.scan(min..max, 1000).await?
					}
				};
				// If there are key-value entries then fetch them
				if !res.is_empty() {
					// Get total results
					let n = res.len();
					// Exit when settled
					if n == 0 {
						break;
					}
					// Loop over results
					for (i, (k, _)) in res.into_iter().enumerate() {
						// Check the context
						if ctx.is_done() {
							break;
						}
						// Ready the next
						if n == i + 1 {
							nxt = Some(k.clone());
						}
						// Parse the data from the store
						let gra: crate::key::graph::Graph = (&k).into();
						// Fetch the data from the store
						let key = thing::new(opt.ns(), opt.db(), gra.ft, &gra.fk);
						let val = ctx.clone_transaction()?.lock().await.get(key).await?;
						let rid = Thing::from((gra.ft, gra.fk));
						let mut ctx = Context::new(ctx);
						ctx.add_thing(&rid);
						// Parse the data from the store
						let val = Operable::Value(match val {
							Some(v) => Value::from(v),
							None => Value::None,
						});
						// Process the record
						ite.process(&ctx, opt, stm, val).await;
					}
					continue;
				}
				break;
			}
		}
		Ok(())
	}

	async fn iterate_index(
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
		table: Table,
		plan: Plan,
		ite: &mut Iterator,
	) -> Result<(), Error> {
		let txn = ctx.clone_transaction()?;
		// Check that the table exists
		txn.lock().await.check_ns_db_tb(opt.ns(), opt.db(), &table.0, opt.strict).await?;
		let mut iterator = plan.new_iterator(opt, &txn).await?;
		let mut things = iterator.next_batch(&txn, 1000).await?;
		while !things.is_empty() {
			// Check if the context is finished
			if ctx.is_done() {
				break;
			}

			for thing in things {
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
				let mut ctx = Context::new(&ctx);
				ctx.add_thing(&rid);
				// Parse the data from the store
				let val = Operable::Value(match val {
					Some(v) => Value::from(v),
					None => Value::None,
				});
				// Process the document record
				ite.process(&ctx, opt, stm, val).await;
			}

			// Collect the next batch of ids
			things = iterator.next_batch(&txn, 1000).await?;
		}
		Ok(())
	}
}
