use crate::ctx::Context;
use crate::ctx::{Canceller, MutableContext};
#[cfg(not(target_arch = "wasm32"))]
use crate::dbs::distinct::AsyncDistinct;
use crate::dbs::distinct::SyncDistinct;
use crate::dbs::plan::Plan;
use crate::dbs::result::Results;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::idx::planner::iterators::{IteratorRecord, IteratorRef};
use crate::idx::planner::IterationStage;
use crate::sql::edges::Edges;
use crate::sql::table::Table;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use crate::sql::{Id, IdRange};
use reblessive::tree::Stk;
#[cfg(not(target_arch = "wasm32"))]
use reblessive::TreeStack;
use std::mem;
use std::sync::Arc;

const TARGET: &str = "surrealdb::core::dbs";

#[derive(Clone)]
pub(crate) enum Iterable {
	Value(Value),
	Table(Table),
	Thing(Thing),
	TableRange(String, IdRange),
	Edges(Edges),
	Defer(Thing),
	Mergeable(Thing, Value),
	Relatable(Thing, Thing, Thing, Option<Value>),
	Index(Table, IteratorRef),
}

#[derive(Debug)]
pub(crate) struct Processed {
	pub(crate) rid: Option<Arc<Thing>>,
	pub(crate) ir: Option<Arc<IteratorRecord>>,
	pub(crate) val: Operable,
}

#[derive(Debug)]
pub(crate) enum Operable {
	Value(Arc<Value>),
	Mergeable(Arc<Value>, Arc<Value>),
	Relatable(Thing, Arc<Value>, Thing, Option<Arc<Value>>),
}

#[derive(Debug)]
pub(crate) enum Workable {
	Normal,
	Insert(Arc<Value>),
	Relate(Thing, Thing, Option<Arc<Value>>),
}

#[derive(Default)]
pub(crate) struct Iterator {
	// Iterator status
	run: Canceller,
	// Iterator limit value
	limit: Option<u32>,
	// Iterator start value
	start: Option<u32>,
	// Iterator runtime error
	error: Option<Error>,
	// Iterator output results
	results: Results,
	// Iterator input values
	entries: Vec<Iterable>,
	// Set if the iterator can be cancelled once it reaches start/limit
	cancel_on_limit: Option<u32>,
}

impl Clone for Iterator {
	fn clone(&self) -> Self {
		Self {
			run: self.run.clone(),
			limit: self.limit,
			start: self.start,
			error: None,
			results: Results::default(),
			entries: self.entries.clone(),
			cancel_on_limit: None,
		}
	}
}

impl Iterator {
	/// Creates a new iterator
	pub fn new() -> Self {
		Self::default()
	}

	/// Ingests an iterable for processing
	pub fn ingest(&mut self, val: Iterable) {
		self.entries.push(val)
	}

	/// Prepares a value for processing
	pub async fn prepare(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		val: Value,
	) -> Result<(), Error> {
		// Match the values
		match val {
			Value::Table(v) => match stm.data() {
				// There is a data clause so fetch a record id
				Some(data) => match stm {
					Statement::Create(_) => {
						let id = match data.rid(stk, ctx, opt).await? {
							// Generate a new id from the id field
							Some(id) => id.generate(&v, false)?,
							// Generate a new random table id
							None => v.generate(),
						};
						self.ingest(Iterable::Thing(id))
					}
					_ => {
						// Ingest the table for scanning
						self.ingest(Iterable::Table(v))
					}
				},
				// There is no data clause so create a record id
				None => match stm {
					Statement::Create(_) => {
						// Generate a new random table id
						self.ingest(Iterable::Thing(v.generate()))
					}
					_ => {
						// Ingest the table for scanning
						self.ingest(Iterable::Table(v))
					}
				},
			},
			Value::Thing(v) => {
				// Check if there is a data clause
				if let Some(data) = stm.data() {
					// Check if there is an id field specified
					if let Some(id) = data.rid(stk, ctx, opt).await? {
						// Check to see the type of the id
						match id {
							// The id is a match, so don't error
							Value::Thing(id) if id == v => (),
							// The id does not match
							id => {
								return Err(Error::IdMismatch {
									value: id.to_string(),
								});
							}
						}
					}
				}
				// Add the record to the iterator
				match &v.id {
					Id::Range(r) => {
						match stm {
							Statement::Create(_) => {
								return Err(Error::InvalidStatementTarget {
									value: v.to_string(),
								});
							}
							_ => {
								self.ingest(Iterable::TableRange(v.tb, *r.to_owned()));
							}
						};
					}
					_ => {
						match stm {
							Statement::Create(_) => {
								self.ingest(Iterable::Defer(v));
							}
							_ => {
								self.ingest(Iterable::Thing(v));
							}
						};
					}
				}
			}
			Value::Mock(v) => {
				// Check if there is a data clause
				if let Some(data) = stm.data() {
					// Check if there is an id field specified
					if let Some(id) = data.rid(stk, ctx, opt).await? {
						return Err(Error::IdMismatch {
							value: id.to_string(),
						});
					}
				}
				// Add the records to the iterator
				for v in v {
					self.ingest(Iterable::Thing(v))
				}
			}
			Value::Edges(v) => {
				// Check if this is a create statement
				if let Statement::Create(_) = stm {
					return Err(Error::InvalidStatementTarget {
						value: v.to_string(),
					});
				}
				// Check if there is a data clause
				if let Some(data) = stm.data() {
					// Check if there is an id field specified
					if let Some(id) = data.rid(stk, ctx, opt).await? {
						return Err(Error::IdMismatch {
							value: id.to_string(),
						});
					}
				}
				// Add the record to the iterator
				self.ingest(Iterable::Edges(*v));
			}
			Value::Object(v) => {
				// Check if there is a data clause
				if let Some(data) = stm.data() {
					// Check if there is an id field specified
					if let Some(id) = data.rid(stk, ctx, opt).await? {
						return Err(Error::IdMismatch {
							value: id.to_string(),
						});
					}
				}
				// Check if the object has an id field
				match v.rid() {
					Some(id) => {
						// Add the record to the iterator
						self.ingest(Iterable::Thing(id))
					}
					None => {
						return Err(Error::InvalidStatementTarget {
							value: v.to_string(),
						});
					}
				}
			}
			Value::Array(v) => {
				// Check if there is a data clause
				if let Some(data) = stm.data() {
					// Check if there is an id field specified
					if let Some(id) = data.rid(stk, ctx, opt).await? {
						return Err(Error::IdMismatch {
							value: id.to_string(),
						});
					}
				}
				// Add the records to the iterator
				for v in v {
					match v {
						Value::Thing(v) => self.ingest(Iterable::Thing(v)),
						Value::Edges(v) => self.ingest(Iterable::Edges(*v)),
						Value::Object(v) => match v.rid() {
							Some(v) => self.ingest(Iterable::Thing(v)),
							None => {
								return Err(Error::InvalidStatementTarget {
									value: v.to_string(),
								})
							}
						},
						_ => {
							return Err(Error::InvalidStatementTarget {
								value: v.to_string(),
							})
						}
					}
				}
			}
			v => {
				return Err(Error::InvalidStatementTarget {
					value: v.to_string(),
				})
			}
		};
		// All ingested ok
		Ok(())
	}

	/// Process the records and output
	pub async fn output(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Log the statement
		trace!(target: TARGET, statement = %stm, "Iterating statement");
		// Enable context override
		let mut cancel_ctx = MutableContext::new(ctx);
		self.run = cancel_ctx.add_cancel();
		let mut cancel_ctx = cancel_ctx.freeze();
		// Process the query LIMIT clause
		self.setup_limit(stk, &cancel_ctx, opt, stm).await?;
		// Process the query START clause
		self.setup_start(stk, &cancel_ctx, opt, stm).await?;
		// Prepare the results with possible optimisations on groups
		self.results = self.results.prepare(
			#[cfg(storage)]
			ctx,
			stm,
		)?;
		// Extract the expected behaviour depending on the presence of EXPLAIN with or without FULL
		let mut plan = Plan::new(ctx, stm, &self.entries, &self.results);
		if plan.do_iterate {
			// Process prepared values
			if let Some(qp) = ctx.get_query_planner() {
				while let Some(s) = qp.next_iteration_stage().await {
					let is_last = matches!(s, IterationStage::Iterate(_));
					let mut c = MutableContext::unfreeze(cancel_ctx)?;
					c.set_iteration_stage(s);
					cancel_ctx = c.freeze();
					if !is_last {
						self.clone().iterate(stk, &cancel_ctx, opt, stm).await?;
					};
				}
			}
			self.iterate(stk, &cancel_ctx, opt, stm).await?;
			// Return any document errors
			if let Some(e) = self.error.take() {
				return Err(e);
			}
			// Process any SPLIT clause
			self.output_split(stk, ctx, opt, stm).await?;
			// Process any GROUP clause
			if let Results::Groups(g) = &mut self.results {
				self.results = Results::Memory(g.output(stk, ctx, opt, stm).await?);
			}

			// Process any ORDER clause
			if let Some(orders) = stm.order() {
				self.results.sort(orders);
			}

			// Process any START & LIMIT clause
			self.results.start_limit(self.start, self.limit);

			if let Some(e) = &mut plan.explanation {
				e.add_fetch(self.results.len());
			} else {
				// Process any FETCH clause
				self.output_fetch(stk, ctx, opt, stm).await?;
			}
		}

		// Extract the output from the result
		let mut results = self.results.take()?;

		// Output the explanation if any
		if let Some(e) = plan.explanation {
			results.clear();
			for v in e.output() {
				results.push(v)
			}
		}

		// Output the results
		Ok(results.into())
	}

	#[inline]
	pub(crate) async fn setup_limit(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Option<u32>, Error> {
		if self.limit.is_none() {
			if let Some(v) = stm.limit() {
				self.limit = Some(v.process(stk, ctx, opt, None).await?);
			}
		}
		Ok(self.limit)
	}

	#[inline]
	async fn setup_start(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		if let Some(v) = stm.start() {
			self.start = Some(v.process(stk, ctx, opt, None).await?);
		}
		Ok(())
	}

	/// Check if the iteration can be limited per iterator
	#[cfg(not(target_arch = "wasm32"))]
	fn check_set_start_limit(&mut self, ctx: &Context, stm: &Statement<'_>) -> bool {
		// If there are groups we can't
		if stm.group().is_some() {
			return false;
		}
		// If there is no specified order, we can
		if stm.order().is_none() {
			return true;
		}
		// If there is more than 1 iterator, we can't
		if self.entries.len() != 1 {
			return false;
		}
		// If the iterator is backed by a sorted index
		// and the sorting matches the first ORDER entry, we can
		if let Some(Iterable::Index(_, irf)) = self.entries.first() {
			if let Some(qp) = ctx.get_query_planner() {
				if qp.is_order(irf) {
					return true;
				}
			}
		}
		false
	}

	#[cfg(not(target_arch = "wasm32"))]
	fn compute_start_limit(&mut self, ctx: &Context, stm: &Statement<'_>) {
		if self.check_set_start_limit(ctx, stm) {
			if let Some(l) = self.limit {
				if let Some(s) = self.start {
					self.cancel_on_limit = Some(l + s);
				} else {
					self.cancel_on_limit = Some(l);
				}
			}
		}
	}

	#[inline]
	async fn output_split(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		if let Some(splits) = stm.split() {
			// Loop over each split clause
			for split in splits.iter() {
				// Get the query result
				let res = self.results.take()?;
				// Loop over each value
				for obj in &res {
					// Get the value at the path
					let val = obj.pick(split);
					// Set the value at the path
					match val {
						Value::Array(v) => {
							for val in v {
								// Make a copy of object
								let mut obj = obj.clone();
								// Set the value at the path
								obj.set(stk, ctx, opt, split, val).await?;
								// Add the object to the results
								self.results.push(stk, ctx, opt, stm, obj).await?;
							}
						}
						_ => {
							// Make a copy of object
							let mut obj = obj.clone();
							// Set the value at the path
							obj.set(stk, ctx, opt, split, val).await?;
							// Add the object to the results
							self.results.push(stk, ctx, opt, stm, obj).await?;
						}
					}
				}
			}
		}
		Ok(())
	}

	#[inline]
	async fn output_fetch(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		if let Some(fetchs) = stm.fetch() {
			let mut idioms = Vec::with_capacity(fetchs.0.len());
			for fetch in fetchs.iter() {
				fetch.compute(stk, ctx, opt, &mut idioms).await?;
			}
			for i in &idioms {
				let mut values = self.results.take()?;
				// Loop over each result value
				for obj in &mut values {
					// Fetch the value at the path
					stk.run(|stk| obj.fetch(stk, ctx, opt, i)).await?;
				}
				self.results = values.into();
			}
		}
		Ok(())
	}

	#[cfg(target_arch = "wasm32")]
	async fn iterate(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Prevent deep recursion
		let opt = &opt.dive(4)?;
		// If any iterator requires distinct, we new to create a global distinct instance
		let mut distinct = SyncDistinct::new(ctx);
		// Process all prepared values
		for v in mem::take(&mut self.entries) {
			v.iterate(stk, ctx, opt, stm, self, distinct.as_mut()).await?;
		}
		// Everything processed ok
		Ok(())
	}

	#[cfg(not(target_arch = "wasm32"))]
	async fn iterate(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Compute iteration limits
		self.compute_start_limit(ctx, stm);
		// Prevent deep recursion
		let opt = &opt.dive(4)?;
		// Check if iterating in parallel
		match stm.parallel() {
			// Run statements sequentially
			false => {
				// If any iterator requires distinct, we need to create a global distinct instance
				let mut distinct = SyncDistinct::new(ctx);
				// Process all prepared values
				for v in mem::take(&mut self.entries) {
					v.iterate(stk, ctx, opt, stm, self, distinct.as_mut()).await?;
				}
				// Everything processed ok
				Ok(())
			}
			// Run statements in parallel
			true => {
				// If any iterator requires distinct, we need to create a global distinct instance
				let distinct = AsyncDistinct::new(ctx);
				// Create a new executor
				let e = executor::Executor::new();
				// Take all of the iterator values
				let vals = mem::take(&mut self.entries);
				// Create a channel to shutdown
				let (end, exit) = channel::bounded::<()>(1);
				// Create an unbounded channel
				let (chn, docs) = channel::bounded(*crate::cnf::MAX_CONCURRENT_TASKS);
				// Create an async closure for prepared values
				let adocs = async {
					// Process all prepared values
					for v in vals {
						// Distinct is passed only for iterators that really requires it
						let chn_clone = chn.clone();
						let distinct_clone = distinct.clone();
						e.spawn(async move {
							let mut stack = TreeStack::new();
							stack
								.enter(|stk| {
									v.channel(stk, ctx, opt, stm, chn_clone, distinct_clone)
								})
								.finish()
								.await
						})
						// Ensure we detach the spawned task
						.detach();
					}
					// Drop the uncloned channel instance
					drop(chn);
				};
				// Create an unbounded channel
				let (chn, vals) = channel::bounded(*crate::cnf::MAX_CONCURRENT_TASKS);
				// Create an async closure for received values
				let avals = async {
					// Process all received values
					while let Ok(pro) = docs.recv().await {
						let chn_clone = chn.clone();
						e.spawn(async move {
							let mut stack = TreeStack::new();
							stack
								.enter(|stk| Document::compute(stk, ctx, opt, stm, chn_clone, pro))
								.finish()
								.await
						})
						// Ensure we detach the spawned task
						.detach();
					}
					// Drop the uncloned channel instance
					drop(chn);
				};
				// Create an async closure to process results
				let aproc = async {
					// Process all processed values
					while let Ok(r) = vals.recv().await {
						self.result(stk, ctx, opt, stm, r).await;
					}
					// Shutdown the executor
					let _ = end.send(()).await;
				};
				// Run all executor tasks
				let fut = e.run(exit.recv());
				// Wait for all closures
				let res = futures::join!(adocs, avals, aproc, fut);
				// Consume executor error
				let _ = res.3;
				// Everything processed ok
				Ok(())
			}
		}
	}

	/// Process a new record Thing and Value
	pub async fn process(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		pro: Processed,
	) {
		// Process the document
		let res = stk.run(|stk| Document::process(stk, ctx, opt, stm, pro)).await;
		// Process the result
		self.result(stk, ctx, opt, stm, res).await;
	}

	/// Accept a processed record result
	async fn result(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		res: Result<Value, Error>,
	) {
		// Process the result
		match res {
			Err(Error::Ignore) => {
				return;
			}
			Err(e) => {
				self.error = Some(e);
				self.run.cancel();
				return;
			}
			Ok(v) => {
				if let Err(e) = self.results.push(stk, ctx, opt, stm, v).await {
					self.error = Some(e);
					self.run.cancel();
					return;
				}
			}
		}
		// Check if we have enough results
		if let Some(l) = self.cancel_on_limit {
			if self.results.len() == l as usize {
				self.run.cancel()
			}
		}
	}
}
