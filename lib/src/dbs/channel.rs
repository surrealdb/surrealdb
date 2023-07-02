use crate::ctx::Context;
use crate::dbs::Iterable;
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
use channel::Sender;
use std::ops::Bound;

impl Iterable {
	#[allow(dead_code)]
	pub(crate) async fn channel(
		self,
		ctx: &Context<'_>,
		opt: &Options,
		_stm: &Statement<'_>,
		chn: Sender<(Option<Thing>, Operable)>,
	) -> Result<(), Error> {
		if ctx.is_ok() {
			match self {
				Iterable::Value(v) => Self::channel_value(ctx, opt, v, chn).await?,
				Iterable::Thing(v) => Self::channel_thing(ctx, opt, v, chn).await?,
				Iterable::Table(v) => Self::channel_table(ctx, opt, v, chn).await?,
				Iterable::Range(v) => Self::channel_range(ctx, opt, v, chn).await?,
				Iterable::Edges(e) => Self::channel_edge(ctx, opt, e, chn).await?,
				Iterable::Index(t, p) => Self::channel_index(ctx, opt, t, p, chn).await?,
				Iterable::Mergeable(v, o) => Self::channel_mergeable(ctx, opt, v, o, chn).await?,
				Iterable::Relatable(f, v, w) => {
					Self::channel_relatable(ctx, opt, f, v, w, chn).await?
				}
			}
		}
		Ok(())
	}

	async fn channel_value(
		_ctx: &Context<'_>,
		_opt: &Options,
		v: Value,
		chn: Sender<(Option<Thing>, Operable)>,
	) -> Result<(), Error> {
		// Pass the value through
		let val = Operable::Value(v);
		// Process the document record
		chn.send((None, val)).await?;
		// Everything ok
		Ok(())
	}

	async fn channel_thing(
		ctx: &Context<'_>,
		opt: &Options,
		v: Thing,
		chn: Sender<(Option<Thing>, Operable)>,
	) -> Result<(), Error> {
		// Clone transaction
		let txn = ctx.try_clone_transaction()?;
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
		chn.send((Some(v), val)).await?;
		// Everything ok
		Ok(())
	}

	async fn channel_mergeable(
		ctx: &Context<'_>,
		opt: &Options,
		v: Thing,
		o: Value,
		chn: Sender<(Option<Thing>, Operable)>,
	) -> Result<(), Error> {
		// Clone transaction
		let txn = ctx.try_clone_transaction()?;
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
		chn.send((Some(v), val)).await?;
		// Everything ok
		Ok(())
	}

	async fn channel_relatable(
		ctx: &Context<'_>,
		opt: &Options,
		f: Thing,
		v: Thing,
		w: Thing,
		chn: Sender<(Option<Thing>, Operable)>,
	) -> Result<(), Error> {
		// Clone transaction
		let txn = ctx.try_clone_transaction()?;
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
		chn.send((Some(v), val)).await?;
		// Everything ok
		Ok(())
	}

	async fn channel_table(
		ctx: &Context<'_>,
		opt: &Options,
		v: Table,
		chn: Sender<(Option<Thing>, Operable)>,
	) -> Result<(), Error> {
		// Clone transaction
		let txn = ctx.try_clone_transaction()?;
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
					let mut child_ctx = Context::new(ctx);
					child_ctx.add_thing(&rid);
					// Process the record
					chn.send((Some(rid), val)).await?;
				}
				continue;
			}
			break;
		}
		// Everything ok
		Ok(())
	}

	async fn channel_range(
		ctx: &Context<'_>,
		opt: &Options,
		v: Range,
		chn: Sender<(Option<Thing>, Operable)>,
	) -> Result<(), Error> {
		// Clone transaction
		let txn = ctx.try_clone_transaction()?;
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
					chn.send((Some(rid), val)).await?;
				}
				continue;
			}
			break;
		}
		// Everything ok
		Ok(())
	}

	async fn channel_edge(
		ctx: &Context<'_>,
		opt: &Options,
		e: Edges,
		chn: Sender<(Option<Thing>, Operable)>,
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
						ctx.try_clone_transaction()?.lock().await.scan(min..max, 1000).await?
					}
					Some(ref mut beg) => {
						beg.push(0x00);
						let min = beg.clone();
						let max = end.clone();
						ctx.try_clone_transaction()?.lock().await.scan(min..max, 1000).await?
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
						let val = ctx.try_clone_transaction()?.lock().await.get(key).await?;
						let rid = Thing::from((gra.ft, gra.fk));
						let mut ctx = Context::new(ctx);
						ctx.add_thing(&rid);
						// Parse the data from the store
						let val = Operable::Value(match val {
							Some(v) => Value::from(v),
							None => Value::None,
						});
						// Process the record
						chn.send((Some(rid), val)).await?;
					}
					continue;
				}
				break;
			}
		}
		// Everything ok
		Ok(())
	}

	async fn channel_index(
		ctx: &Context<'_>,
		opt: &Options,
		table: Table,
		plan: Plan,
		chn: Sender<(Option<Thing>, Operable)>,
	) -> Result<(), Error> {
		let txn = ctx.try_clone_transaction()?;
		// Check that the table exists
		txn.lock().await.check_ns_db_tb(opt.ns(), opt.db(), &table.0, opt.strict).await?;
		let exe = ctx.get_query_executor(&table.0);
		if let Some(exe) = exe {
			let mut iterator = plan.new_iterator(opt, &txn, exe).await?;
			let mut things = iterator.next_batch(&txn, 1000).await?;
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
					let mut ctx = Context::new(ctx);
					ctx.add_thing(&rid);
					ctx.add_doc_id(doc_id);
					// Parse the data from the store
					let val = Operable::Value(match val {
						Some(v) => Value::from(v),
						None => Value::None,
					});
					// Process the document record
					chn.send((Some(rid), val)).await?;
				}

				// Collect the next batch of ids
				things = iterator.next_batch(&txn, 1000).await?;
			}
			// Everything ok
			Ok(())
		} else {
			Err(Error::QueryNotExecutedDetail {
				message: "The QueryExecutor has not been found.".to_string(),
			})
		}
	}
}
