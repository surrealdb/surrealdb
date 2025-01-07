use crate::ctx::Context;
use crate::ctx::{Canceller, MutableContext};
use crate::dbs::distinct::SyncDistinct;
use crate::dbs::plan::Plan;
use crate::dbs::result::Results;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::idx::planner::iterators::{IteratorRecord, IteratorRef};
use crate::idx::planner::IterationStage;
use crate::sql::array::Array;
use crate::sql::edges::Edges;
use crate::sql::mock::Mock;
use crate::sql::object::Object;
use crate::sql::table::Table;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use crate::sql::{Fields, Id, IdRange};
use reblessive::tree::Stk;
use std::mem;
use std::sync::Arc;

const TARGET: &str = "surrealdb::core::dbs";

#[derive(Clone)]
pub(crate) enum Iterable {
	/// Any [Value] which does not exist in storage. This
	/// could be the result of a query, an arbitrary
	/// SurrealQL value, object, or array of values.
	Value(Value),
	/// An iterable which does not actually fetch the record
	/// data from storage. This is used in CREATE statements
	/// where we attempt to write data without first checking
	/// if the record exists, throwing an error on failure.
	Defer(Thing),
	/// An iterable whose Record ID needs to be generated
	/// before processing. This is used in CREATE statements
	/// when generating a new id, or generating an id based
	/// on the id field which is specified within the data.
	Yield(Table),
	/// An iterable which needs to fetch the data of a
	/// specific record before processing the document.
	Thing(Thing),
	/// An iterable which needs to fetch the related edges
	/// of a record before processing each document.
	Edges(Edges),
	/// An iterable which needs to iterate over the records
	/// in a table before processing each document. When the
	/// 2nd argument is true, we iterate over keys only.
	Table(Table, bool),
	/// An iterable which fetches a specific range of records
	/// from storage, used in range and time-series scenarios.
	/// When the 2nd argument is true, we iterate over keys only.
	Range(String, IdRange, bool),
	/// An iterable which fetches a record from storage, and
	/// which has the specific value to update the record with.
	/// This is used in INSERT statements, where each value
	/// passed in to the iterable is unique for each record.
	Mergeable(Thing, Value),
	/// An iterable which fetches a record from storage, and
	/// which has the specific value to update the record with.
	/// This is used in RELATE statements. The optional value
	/// is used in INSERT RELATION statements, where each value
	/// passed in to the iterable is unique for each record.
	Relatable(Thing, Thing, Thing, Option<Value>),
	/// An iterable which iterates over an index range for a
	/// table, which then fetches the corresponding records
	/// which are matched within the index.
	/// When the 3rd argument is true, we iterate over keys only.
	Index(Table, IteratorRef, bool),
}

#[derive(Debug)]
pub(crate) enum Operable {
	Value(Arc<Value>),
	Insert(Arc<Value>, Arc<Value>),
	Relate(Thing, Arc<Value>, Thing, Option<Arc<Value>>),
}

#[derive(Debug)]
pub(crate) enum Workable {
	Normal,
	Insert(Arc<Value>),
	Relate(Thing, Thing, Option<Arc<Value>>),
}

#[derive(Debug)]
pub(crate) struct Processed {
	/// Whether this document only fetched keys
	pub(crate) keys_only: bool,
	/// Whether this document needs to have an ID generated
	pub(crate) generate: Option<Table>,
	/// The record id for this document that should be processed
	pub(crate) rid: Option<Arc<Thing>>,
	/// The record data for this document that should be processed
	pub(crate) val: Operable,
	/// The record iterator for this document, used in index scans
	pub(crate) ir: Option<Arc<IteratorRecord>>,
}

#[derive(Default)]
pub(crate) struct Iterator {
	/// Iterator status
	run: Canceller,
	/// Total documents processed
	count: u64,
	/// Iterator limit value
	limit: Option<u32>,
	/// Iterator start value
	start: Option<u32>,
	/// Iterator runtime error
	error: Option<Error>,
	/// Iterator output results
	results: Results,
	/// Iterator input values
	entries: Vec<Iterable>,
	/// Should we always return a record?
	guaranteed: Option<Iterable>,
	/// Set if the iterator can be cancelled once it reaches start/limit
	cancel_on_limit: Option<u32>,
}

impl Clone for Iterator {
	fn clone(&self) -> Self {
		Self {
			run: self.run.clone(),
			count: 0,
			limit: self.limit,
			start: self.start,
			error: None,
			results: Results::default(),
			entries: self.entries.clone(),
			guaranteed: None,
			cancel_on_limit: None,
		}
	}
}

impl Iterator {
	/// Creates a new iterator
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// Ingests an iterable for processing
	pub(crate) fn ingest(&mut self, val: Iterable) {
		self.entries.push(val)
	}

	/// Prepares a value for processing
	pub(crate) fn prepare(&mut self, stm: &Statement<'_>, val: Value) -> Result<(), Error> {
		// Match the values
		match val {
			Value::Mock(v) => self.prepare_mock(stm, v)?,
			Value::Table(v) => self.prepare_table(stm, v)?,
			Value::Edges(v) => self.prepare_edges(stm, *v)?,
			Value::Object(v) => self.prepare_object(stm, v)?,
			Value::Array(v) => self.prepare_array(stm, v)?,
			Value::Thing(v) => match v.is_range() {
				true => self.prepare_range(stm, v, false)?,
				false => self.prepare_thing(stm, v)?,
			},
			v => {
				return Err(Error::InvalidStatementTarget {
					value: v.to_string(),
				})
			}
		};
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	pub(crate) fn prepare_table(&mut self, stm: &Statement<'_>, v: Table) -> Result<(), Error> {
		// Add the record to the iterator
		match stm.is_deferable() {
			true => self.ingest(Iterable::Yield(v)),
			false => match stm.is_guaranteed() {
				false => self.ingest(Iterable::Table(v, false)),
				true => {
					self.guaranteed = Some(Iterable::Yield(v.clone()));
					self.ingest(Iterable::Table(v, false))
				}
			},
		}
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	pub(crate) fn prepare_thing(&mut self, stm: &Statement<'_>, v: Thing) -> Result<(), Error> {
		// Add the record to the iterator
		match stm.is_deferable() {
			true => self.ingest(Iterable::Defer(v)),
			false => self.ingest(Iterable::Thing(v)),
		}
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	pub(crate) fn prepare_mock(&mut self, stm: &Statement<'_>, v: Mock) -> Result<(), Error> {
		// Add the records to the iterator
		for v in v {
			match stm.is_deferable() {
				true => self.ingest(Iterable::Defer(v)),
				false => self.ingest(Iterable::Thing(v)),
			}
		}
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	pub(crate) fn prepare_edges(&mut self, stm: &Statement<'_>, v: Edges) -> Result<(), Error> {
		// Check if this is a create statement
		if stm.is_create() {
			return Err(Error::InvalidStatementTarget {
				value: v.to_string(),
			});
		}
		// Add the record to the iterator
		self.ingest(Iterable::Edges(v));
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	pub(crate) fn prepare_range(
		&mut self,
		stm: &Statement<'_>,
		v: Thing,
		keys: bool,
	) -> Result<(), Error> {
		// Check if this is a create statement
		if stm.is_create() {
			return Err(Error::InvalidStatementTarget {
				value: v.to_string(),
			});
		}
		// Add the record to the iterator
		if let (tb, Id::Range(v)) = (v.tb, v.id) {
			self.ingest(Iterable::Range(tb, *v, keys));
		}
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	pub(crate) fn prepare_object(&mut self, stm: &Statement<'_>, v: Object) -> Result<(), Error> {
		// Add the record to the iterator
		match v.rid() {
			// This object has an 'id' field
			Some(v) => match stm.is_deferable() {
				true => self.ingest(Iterable::Defer(v)),
				false => self.ingest(Iterable::Thing(v)),
			},
			// This object has no 'id' field
			None => {
				return Err(Error::InvalidStatementTarget {
					value: v.to_string(),
				});
			}
		}
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	pub(crate) fn prepare_array(&mut self, stm: &Statement<'_>, v: Array) -> Result<(), Error> {
		// Add the records to the iterator
		for v in v {
			match v {
				Value::Mock(v) => self.prepare_mock(stm, v)?,
				Value::Table(v) => self.prepare_table(stm, v)?,
				Value::Edges(v) => self.prepare_edges(stm, *v)?,
				Value::Object(v) => self.prepare_object(stm, v)?,
				Value::Thing(v) => match v.is_range() {
					true => self.prepare_range(stm, v, false)?,
					false => self.prepare_thing(stm, v)?,
				},
				_ => {
					return Err(Error::InvalidStatementTarget {
						value: v.to_string(),
					})
				}
			}
		}
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
			self.start,
			self.limit,
		)?;
		// Extract the expected behaviour depending on the presence of EXPLAIN with or without FULL
		let mut plan = Plan::new(ctx, stm, &self.entries, &self.results);
		// Check if we actually need to process and iterate over the results
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
			// Process all documents
			self.iterate(stk, &cancel_ctx, opt, stm).await?;
			// Return any document errors
			if let Some(e) = self.error.take() {
				return Err(e);
			}
			// If no results, then create a record
			if self.results.is_empty() {
				// Check if a guaranteed record response is expected
				if let Some(guaranteed) = self.guaranteed.take() {
					// Ingest the pre-defined guaranteed record yield
					self.ingest(guaranteed);
					// Process the pre-defined guaranteed document
					self.iterate(stk, &cancel_ctx, opt, stm).await?;
				}
			}
			// Process any SPLIT AT clause
			self.output_split(stk, ctx, opt, stm).await?;
			// Process any GROUP BY clause
			self.output_group(stk, ctx, opt, stm).await?;
			// Process any ORDER BY clause
			if let Some(orders) = stm.order() {
				#[cfg(not(target_arch = "wasm32"))]
				self.results.sort(orders).await?;
				#[cfg(target_arch = "wasm32")]
				self.results.sort(orders);
			}
			// Process any START & LIMIT clause
			self.results.start_limit(self.start, self.limit).await?;
			// Process any FETCH clause
			if let Some(e) = &mut plan.explanation {
				e.add_fetch(self.results.len());
			} else {
				self.output_fetch(stk, ctx, opt, stm).await?;
			}
		}

		// Extract the output from the result
		let mut results = self.results.take().await?;

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
		if let Some(Iterable::Index(_, irf, _)) = self.entries.first() {
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
				let res = self.results.take().await?;
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

	async fn output_group(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Process any GROUP clause
		if let Results::Groups(g) = &mut self.results {
			self.results = Results::Memory(g.output(stk, ctx, opt, stm).await?);
		}
		// Everything ok
		Ok(())
	}

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
				let mut values = self.results.take().await?;
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
		// If any iterator requires distinct, we need to create a global distinct instance
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
		let opt = opt.dive(4)?;
		// If any iterator requires distinct, we need to create a global distinct instance
		let mut distinct = SyncDistinct::new(ctx);
		// Process all prepared values
		for v in mem::take(&mut self.entries) {
			v.iterate(stk, ctx, &opt, stm, self, distinct.as_mut()).await?;
		}
		// Everything processed ok
		Ok(())
	}

	/// Process a new record Thing and Value
	pub async fn process(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		pro: Processed,
	) -> Result<(), Error> {
		// Check if this is a count all
		let count_all = stm.expr().is_some_and(Fields::is_count_all_only);
		// Process the document
		let res = if count_all && pro.keys_only {
			Ok(map! { "count".to_string() => Value::from(1) }.into())
		} else {
			stk.run(|stk| Document::process(stk, ctx, opt, stm, pro)).await
		};
		// Process the result
		self.result(stk, ctx, opt, stm, res).await;
		// Everything ok
		Ok(())
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
		// Count the result
		self.count += 1;
		// Periodically yield
		if self.count % 100 == 0 {
			yield_now!();
		}
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
