use crate::ctx::Canceller;
use crate::ctx::Context;
#[cfg(not(target_arch = "wasm32"))]
use crate::dbs::distinct::AsyncDistinct;
use crate::dbs::distinct::SyncDistinct;
use crate::dbs::plan::Plan;
use crate::dbs::result::Results;
use crate::dbs::Statement;
use crate::dbs::{Options, Transaction};
use crate::doc::Document;
use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::planner::executor::IteratorRef;
use crate::idx::planner::IterationStage;
use crate::sql::edges::Edges;
use crate::sql::range::Range;
use crate::sql::table::Table;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use async_recursion::async_recursion;
use std::mem;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) enum Iterable {
	Value(Value),
	Table(Arc<Table>),
	Thing(Thing),
	Range(Range),
	Edges(Edges),
	Defer(Thing),
	Mergeable(Thing, Value),
	Relatable(Thing, Thing, Thing),
	Index(Arc<Table>, IteratorRef),
}

pub(crate) struct Processed {
	pub(crate) ir: Option<IteratorRef>,
	pub(crate) rid: Option<Thing>,
	pub(crate) doc_id: Option<DocId>,
	pub(crate) val: Operable,
}

pub(crate) enum Operable {
	Value(Value),
	Mergeable(Value, Value),
	Relatable(Thing, Value, Thing),
}

pub(crate) enum Workable {
	Normal,
	Insert(Value),
	Relate(Thing, Thing),
}

#[derive(Default)]
pub(crate) struct Iterator {
	// Iterator status
	run: Canceller,
	// Iterator limit value
	limit: Option<usize>,
	// Iterator start value
	start: Option<usize>,
	// Iterator runtime error
	error: Option<Error>,
	// Iterator output results
	results: Results,
	// Iterator input values
	entries: Vec<Iterable>,
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
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		val: Value,
	) -> Result<(), Error> {
		// Match the values
		match val {
			Value::Table(v) => match stm.data() {
				// There is a data clause so fetch a record id
				Some(data) => match stm {
					Statement::Create(_) => {
						let id = match data.rid(ctx, opt, txn).await? {
							// Generate a new id from the id field
							Some(id) => id.generate(&v, false)?,
							// Generate a new random table id
							None => v.generate(),
						};
						self.ingest(Iterable::Thing(id))
					}
					_ => {
						// Ingest the table for scanning
						self.ingest(Iterable::Table(Arc::new(v)))
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
						self.ingest(Iterable::Table(Arc::new(v)))
					}
				},
			},
			Value::Thing(v) => {
				// Check if there is a data clause
				if let Some(data) = stm.data() {
					// Check if there is an id field specified
					if let Some(id) = data.rid(ctx, opt, txn).await? {
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
				match stm {
					Statement::Create(_) => {
						self.ingest(Iterable::Defer(v));
					}
					_ => {
						self.ingest(Iterable::Thing(v));
					}
				};
			}
			Value::Mock(v) => {
				// Check if there is a data clause
				if let Some(data) = stm.data() {
					// Check if there is an id field specified
					if let Some(id) = data.rid(ctx, opt, txn).await? {
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
			Value::Range(v) => {
				// Check if this is a create statement
				if let Statement::Create(_) = stm {
					return Err(Error::InvalidStatementTarget {
						value: v.to_string(),
					});
				}
				// Check if there is a data clause
				if let Some(data) = stm.data() {
					// Check if there is an id field specified
					if let Some(id) = data.rid(ctx, opt, txn).await? {
						return Err(Error::IdMismatch {
							value: id.to_string(),
						});
					}
				}
				// Add the record to the iterator
				self.ingest(Iterable::Range(*v));
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
					if let Some(id) = data.rid(ctx, opt, txn).await? {
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
					if let Some(id) = data.rid(ctx, opt, txn).await? {
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
					if let Some(id) = data.rid(ctx, opt, txn).await? {
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
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Log the statement
		trace!("Iterating: {}", stm);
		// Enable context override
		let mut cancel_ctx = Context::new(ctx);
		self.run = cancel_ctx.add_cancel();
		// Process the query LIMIT clause
		self.setup_limit(&cancel_ctx, opt, txn, stm).await?;
		// Process the query START clause
		self.setup_start(&cancel_ctx, opt, txn, stm).await?;
		// Prepare the results with possible optimisations on groups
		self.results = self.results.prepare(
			#[cfg(any(
				feature = "kv-surrealkv",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
				feature = "kv-speedb"
			))]
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
					cancel_ctx.set_iteration_stage(s);
					if !is_last {
						self.clone().iterate(&cancel_ctx, opt, txn, stm).await?;
					};
				}
			}
			self.iterate(&cancel_ctx, opt, txn, stm).await?;
			// Return any document errors
			if let Some(e) = self.error.take() {
				return Err(e);
			}
			// Process any SPLIT clause
			self.output_split(ctx, opt, txn, stm).await?;

			// Process any GROUP clause
			if let Results::Groups(g) = &mut self.results {
				self.results = Results::Memory(g.output(ctx, opt, txn, stm).await?);
			}

			// Process any ORDER clause
			if let Some(orders) = stm.order() {
				self.results.sort(orders);
			}

			// Process any START & LIMIT clause
			self.results.start_limit(self.start.as_ref(), self.limit.as_ref());

			if let Some(e) = &mut plan.explanation {
				e.add_fetch(self.results.len());
			} else {
				// Process any FETCH clause
				self.output_fetch(ctx, opt, txn, stm).await?;
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
	async fn setup_limit(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		if let Some(v) = stm.limit() {
			self.limit = Some(v.process(ctx, opt, txn, None).await?);
		}
		Ok(())
	}

	#[inline]
	async fn setup_start(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		if let Some(v) = stm.start() {
			self.start = Some(v.process(ctx, opt, txn, None).await?);
		}
		Ok(())
	}

	#[inline]
	async fn output_split(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
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
								obj.set(ctx, opt, txn, split, val).await?;
								// Add the object to the results
								self.results.push(ctx, opt, txn, stm, obj).await?;
							}
						}
						_ => {
							// Make a copy of object
							let mut obj = obj.clone();
							// Set the value at the path
							obj.set(ctx, opt, txn, split, val).await?;
							// Add the object to the results
							self.results.push(ctx, opt, txn, stm, obj).await?;
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
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		if let Some(fetchs) = stm.fetch() {
			for fetch in fetchs.iter() {
				let mut values = self.results.take()?;
				// Loop over each result value
				for obj in &mut values {
					// Fetch the value at the path
					obj.fetch(ctx, opt, txn, fetch).await?;
				}
				self.results = values.into();
			}
		}
		Ok(())
	}

	#[cfg(target_arch = "wasm32")]
	#[async_recursion(?Send)]
	async fn iterate(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Prevent deep recursion
		let opt = &opt.dive(4)?;
		// If any iterator requires distinct, we new to create a global distinct instance
		let mut distinct = SyncDistinct::new(ctx);
		// Process all prepared values
		for v in mem::take(&mut self.entries) {
			v.iterate(ctx, opt, txn, stm, self, distinct.as_mut()).await?;
		}
		// Everything processed ok
		Ok(())
	}

	#[cfg(not(target_arch = "wasm32"))]
	#[async_recursion]
	async fn iterate(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Prevent deep recursion
		let opt = &opt.dive(4)?;
		// Check if iterating in parallel
		match stm.parallel() {
			// Run statements sequentially
			false => {
				// If any iterator requires distinct, we new to create a global distinct instance
				let mut distinct = SyncDistinct::new(ctx);
				// Process all prepared values
				for v in mem::take(&mut self.entries) {
					v.iterate(ctx, opt, txn, stm, self, distinct.as_mut()).await?;
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
				let (chn, docs) = channel::bounded(crate::cnf::MAX_CONCURRENT_TASKS);
				// Create an async closure for prepared values
				let adocs = async {
					// Process all prepared values
					for v in vals {
						// Distinct is passed only for iterators that really requires it
						e.spawn(v.channel(ctx, opt, txn, stm, chn.clone(), distinct.clone()))
							// Ensure we detach the spawned task
							.detach();
					}
					// Drop the uncloned channel instance
					drop(chn);
				};
				// Create an unbounded channel
				let (chn, vals) = channel::bounded(crate::cnf::MAX_CONCURRENT_TASKS);
				// Create an async closure for received values
				let avals = async {
					// Process all received values
					while let Ok(pro) = docs.recv().await {
						e.spawn(Document::compute(ctx, opt, txn, stm, chn.clone(), pro))
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
						self.result(ctx, opt, txn, stm, r).await;
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
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		pro: Processed,
	) {
		// Process the document
		let res = Document::process(ctx, opt, txn, stm, pro).await;
		// Process the result
		self.result(ctx, opt, txn, stm, res).await;
	}

	/// Accept a processed record result
	async fn result(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
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
				if let Err(e) = self.results.push(ctx, opt, txn, stm, v).await {
					self.error = Some(e);
					self.run.cancel();
					return;
				}
			}
		}
		// Check if we can exit
		if stm.group().is_none() && stm.order().is_none() {
			if let Some(l) = self.limit {
				if let Some(s) = self.start {
					if self.results.len() == l + s {
						self.run.cancel()
					}
				} else if self.results.len() == l {
					self.run.cancel()
				}
			}
		}
	}
}
