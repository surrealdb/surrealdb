use std::mem;
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;

use crate::ctx::{Canceller, Context, MutableContext};
use crate::dbs::distinct::SyncDistinct;
use crate::dbs::plan::{Explanation, Plan};
use crate::dbs::result::Results;
use crate::dbs::{Options, Statement};
use crate::doc::{CursorDoc, Document, IgnoreError};
use crate::err::Error;
use crate::expr::lookup::{ComputedLookupSubject, LookupKind};
use crate::expr::{
	self, ControlFlow, Expr, Fields, FlowResultExt, Ident, Literal, Lookup, Mock, Part,
};
use crate::idx::planner::iterators::{IteratorRecord, IteratorRef};
use crate::idx::planner::{
	GrantedPermission, IterationStage, QueryPlanner, RecordStrategy, ScanDirection,
	StatementContext,
};
use crate::val::record::Record;
use crate::val::{Object, RecordId, RecordIdKey, RecordIdKeyRange, Value};

const TARGET: &str = "surrealdb::core::dbs";

#[derive(Clone, Debug)]
pub(crate) enum Iterable {
	/// Any [Value] which does not exist in storage. This
	/// could be the result of a query, an arbitrary
	/// SurrealQL value, object, or array of values.
	Value(Value),
	/// An iterable which does not actually fetch the record
	/// data from storage. This is used in CREATE statements
	/// where we attempt to write data without first checking
	/// if the record exists, throwing an error on failure.
	Defer(RecordId),
	/// An iterable whose Record ID needs to be generated
	/// before processing. This is used in CREATE statements
	/// when generating a new id, or generating an id based
	/// on the id field which is specified within the data.
	Yield(Ident),
	/// An iterable which needs to fetch the data of a
	/// specific record before processing the document.
	Thing(RecordId),
	/// An iterable which needs to fetch the related edges
	/// of a record before processing each document.
	Lookup {
		kind: LookupKind,
		from: RecordId,
		what: Vec<ComputedLookupSubject>,
	},
	/// An iterable which needs to iterate over the records
	/// in a table before processing each document.
	Table(Ident, RecordStrategy, ScanDirection),
	/// An iterable which fetches a specific range of records
	/// from storage, used in range and time-series scenarios.
	Range(String, RecordIdKeyRange, RecordStrategy, ScanDirection),
	/// An iterable which fetches a record from storage, and
	/// which has the specific value to update the record with.
	/// This is used in INSERT statements, where each value
	/// passed in to the iterable is unique for each record.
	Mergeable(RecordId, Value),
	/// An iterable which fetches a record from storage, and
	/// which has the specific value to update the record with.
	/// This is used in RELATE statements. The optional value
	/// is used in INSERT RELATION statements, where each value
	/// passed in to the iterable is unique for each record.
	///
	/// The first field is the rid from which we create, the second is the rid
	/// which is the relation itself and the third is the target of the
	/// relation
	Relatable(RecordId, RecordId, RecordId, Option<Value>),
	/// An iterable which iterates over an index range for a
	/// table, which then fetches the corresponding records
	/// which are matched within the index.
	/// When the 3rd argument is true, we iterate over keys only.
	Index(Ident, IteratorRef, RecordStrategy),
}

#[derive(Debug)]
pub(crate) enum Operable {
	Value(Arc<Record>),
	Insert(Arc<Record>, Arc<Value>),
	Relate(RecordId, Arc<Record>, RecordId, Option<Arc<Value>>),
	Count(usize),
}

#[derive(Debug)]
pub(crate) enum Workable {
	Normal,
	Insert(Arc<Value>),
	Relate(RecordId, RecordId, Option<Arc<Value>>),
}

#[derive(Debug)]
pub(crate) struct Processed {
	/// Whether this document only fetched keys or just count
	pub(crate) rs: RecordStrategy,
	/// Whether this document needs to have an ID generated
	pub(crate) generate: Option<Ident>,
	/// The record id for this document that should be processed
	pub(crate) rid: Option<Arc<RecordId>>,
	/// The record data for this document that should be processed
	pub(crate) val: Operable,
	/// The record iterator for this document, used in index scans
	pub(crate) ir: Option<Arc<IteratorRecord>>,
}

#[derive(Default)]
pub(crate) struct Iterator {
	/// Iterator status
	run: Canceller,
	/// Iterator limit value
	limit: Option<u32>,
	/// Iterator start value
	start: Option<u32>,
	/// Counter of remaining documents that can be skipped processing
	start_skip: Option<usize>,
	/// Iterator runtime error
	error: Option<anyhow::Error>,
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
			limit: self.limit,
			start: self.start,
			start_skip: self.start_skip.map(|_| self.start.unwrap_or(0) as usize),
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
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn prepare(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		planner: &mut QueryPlanner,
		stm_ctx: &StatementContext<'_>,
		val: &Expr,
	) -> Result<()> {
		// Match the values
		match val {
			Expr::Mock(v) => self.prepare_mock(stm_ctx, v).await?,
			Expr::Table(v) => {
				self.prepare_table(ctx, opt, stk, planner, stm_ctx, v.clone()).await?
			}
			Expr::Idiom(x) => {
				// TODO: This needs to be structured better.
				// match against what previously would be an edge.
				if x.len() != 2 {
					return self.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, val).await;
				}

				let Part::Start(Expr::Literal(Literal::RecordId(ref from))) = x[0] else {
					return self.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, val).await;
				};

				let Part::Lookup(ref lookup) = x[1] else {
					return self.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, val).await;
				};

				if lookup.alias.is_none()
					&& lookup.cond.is_none()
					&& lookup.group.is_none()
					&& lookup.limit.is_none()
					&& lookup.order.is_none()
					&& lookup.split.is_none()
					&& lookup.start.is_none()
					&& lookup.expr.is_none()
				{
					// TODO: Do we support `RETURN a:b` here? What do we do when it is not of the
					// right type?
					let from = match from.compute(stk, ctx, opt, doc).await {
						Ok(x) => x,
						Err(ControlFlow::Err(e)) => return Err(e),
						Err(_) => bail!(Error::InvalidControlFlow),
						//
					};
					let mut what = Vec::new();
					for s in lookup.what.iter() {
						what.push(s.compute(stk, ctx, opt, doc, &lookup.kind).await?);
					}
					// idiom matches the Edges pattern.
					self.prepare_lookup(stm_ctx.stm, from, lookup.kind.clone(), what)?;
				}
			}
			Expr::Literal(Literal::Array(array)) => {
				self.prepare_array(stk, ctx, opt, doc, planner, stm_ctx, array).await?
			}
			x => self.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, x).await?,
		};
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	pub(crate) async fn prepare_table(
		&mut self,
		ctx: &Context,
		opt: &Options,
		stk: &mut Stk,
		planner: &mut QueryPlanner,
		stm_ctx: &StatementContext<'_>,
		table: Ident,
	) -> Result<()> {
		// We add the iterable only if we have a permission
		let p = planner.check_table_permission(stm_ctx, &table).await?;
		if matches!(p, GrantedPermission::None) {
			return Ok(());
		}
		// Add the record to the iterator
		if stm_ctx.stm.is_deferable() {
			ctx.get_db(opt).await?;
			self.ingest(Iterable::Yield(table))
		} else {
			if stm_ctx.stm.is_guaranteed() {
				self.guaranteed = Some(Iterable::Yield(table.clone()));
			}
			let db = ctx.get_db(opt).await?;

			planner.add_iterables(&db, stk, stm_ctx, table, p, self).await?;
		}
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	pub(crate) async fn prepare_thing(
		&mut self,
		planner: &mut QueryPlanner,
		ctx: &StatementContext<'_>,
		v: RecordId,
	) -> Result<()> {
		if v.key.is_range() {
			return self.prepare_range(planner, ctx, v).await;
		}
		// We add the iterable only if we have a permission
		if matches!(planner.check_table_permission(ctx, &v.table).await?, GrantedPermission::None) {
			return Ok(());
		}
		// Add the record to the iterator
		match ctx.stm.is_deferable() {
			true => self.ingest(Iterable::Defer(v)),
			false => self.ingest(Iterable::Thing(v)),
		}
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	pub(crate) async fn prepare_mock(
		&mut self,
		ctx: &StatementContext<'_>,
		v: &Mock,
	) -> Result<()> {
		ensure!(!ctx.stm.is_only() || self.is_limit_one_or_zero(), Error::SingleOnlyOutput);
		// Add the records to the iterator
		for (count, v) in v.clone().into_iter().enumerate() {
			if ctx.stm.is_deferable() {
				self.ingest(Iterable::Defer(v))
			} else {
				self.ingest(Iterable::Thing(v))
			}
			// Check if the context is finished
			if ctx.ctx.is_done(count % 100 == 0).await? {
				break;
			}
		}
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	pub(crate) fn prepare_lookup(
		&mut self,
		stm: &Statement<'_>,
		from: RecordId,
		kind: LookupKind,
		what: Vec<ComputedLookupSubject>,
	) -> Result<()> {
		ensure!(!stm.is_only() || self.is_limit_one_or_zero(), Error::SingleOnlyOutput);
		// Check if this is a create statement
		if stm.is_create() {
			// recreate the expression for the error.
			let value = expr::Idiom(vec![
				expr::Part::Start(Expr::Literal(Literal::RecordId(from.into_literal()))),
				expr::Part::Lookup(Lookup {
					kind,
					what: what.into_iter().map(|x| x.into_literal()).collect(),
					..Default::default()
				}),
			])
			.to_string();
			bail!(Error::InvalidStatementTarget {
				value,
			})
		}
		let x = Iterable::Lookup {
			from,
			kind,
			what,
		};
		// Add the record to the iterator
		self.ingest(x);
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	pub(crate) async fn prepare_range(
		&mut self,
		planner: &mut QueryPlanner,
		ctx: &StatementContext<'_>,
		v: RecordId,
	) -> Result<()> {
		// We add the iterable only if we have a permission
		let p = planner.check_table_permission(ctx, &v.table).await?;
		if matches!(p, GrantedPermission::None) {
			return Ok(());
		}
		// Check if this is a create statement
		ensure!(
			!ctx.stm.is_create(),
			Error::InvalidStatementTarget {
				value: v.to_string(),
			}
		);
		// Evaluate if we can only scan keys (rather than keys AND values), or count
		let rs = ctx.check_record_strategy(false, p)?;
		let sc = ctx.check_scan_direction();
		// Add the record to the iterator
		if let (tb, RecordIdKey::Range(v)) = (v.table, v.key) {
			self.ingest(Iterable::Range(tb, *v, rs, sc));
		}
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	pub(crate) fn prepare_object(&mut self, stm: &Statement<'_>, v: Object) -> Result<()> {
		// Add the record to the iterator
		match v.rid() {
			// This object has an 'id' field
			Some(v) => {
				if stm.is_deferable() {
					self.ingest(Iterable::Defer(v))
				} else {
					self.ingest(Iterable::Thing(v))
				}
			}
			// This object has no 'id' field
			None => {
				bail!(Error::InvalidStatementTarget {
					value: v.to_string(),
				});
			}
		}
		// All ingested ok
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn prepare_computed(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		planner: &mut QueryPlanner,
		stm_ctx: &StatementContext<'_>,
		expr: &Expr,
	) -> Result<()> {
		let v = stk.run(|stk| expr.compute(stk, ctx, opt, doc)).await.catch_return()?;
		match v {
			Value::Object(o) if !stm_ctx.stm.is_select() => {
				self.prepare_object(stm_ctx.stm, o)?;
			}
			Value::Table(v) => {
				self.prepare_table(ctx, opt, stk, planner, stm_ctx, v.into()).await?
			}
			Value::RecordId(v) => self.prepare_thing(planner, stm_ctx, v).await?,
			Value::Array(a) => a.into_iter().for_each(|x| self.ingest(Iterable::Value(x))),
			v if stm_ctx.stm.is_select() => self.ingest(Iterable::Value(v)),
			v => {
				bail!(Error::InvalidStatementTarget {
					value: v.to_string(),
				})
			}
		}
		Ok(())
	}

	/// Prepares a value for processing
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn prepare_array(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		planner: &mut QueryPlanner,
		stm_ctx: &StatementContext<'_>,
		v: &[Expr],
	) -> Result<()> {
		ensure!(!stm_ctx.stm.is_only() || self.is_limit_one_or_zero(), Error::SingleOnlyOutput);
		// Add the records to the iterator
		for v in v {
			match v {
				Expr::Mock(v) => self.prepare_mock(stm_ctx, v).await?,
				Expr::Table(v) => {
					self.prepare_table(ctx, opt, stk, planner, stm_ctx, v.clone()).await?
				}
				Expr::Idiom(x) => {
					// match against what previously would be an edge.
					if x.len() != 2 {
						return self
							.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, v)
							.await;
					}

					let Part::Start(Expr::Literal(Literal::RecordId(ref from))) = x[0] else {
						return self
							.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, v)
							.await;
					};

					let Part::Lookup(ref lookup) = x[0] else {
						return self
							.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, v)
							.await;
					};

					if lookup.alias.is_none()
						&& lookup.cond.is_none()
						&& lookup.group.is_none()
						&& lookup.limit.is_none()
						&& lookup.order.is_none()
						&& lookup.split.is_none()
						&& lookup.start.is_none()
						&& lookup.expr.is_none()
					{
						// TODO: Do we support `RETURN a:b` here? What do we do when it is not of
						// the right type?
						let from = match from.compute(stk, ctx, opt, doc).await {
							Ok(x) => x,
							Err(ControlFlow::Err(e)) => return Err(e),
							Err(_) => bail!(Error::InvalidControlFlow),
							//
						};
						let mut what = Vec::new();
						for s in lookup.what.iter() {
							what.push(s.compute(stk, ctx, opt, doc, &lookup.kind).await?);
						}
						// idiom matches the Edges pattern.
						return self.prepare_lookup(stm_ctx.stm, from, lookup.kind.clone(), what);
					}

					self.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, v).await?
				}
				v => self.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, v).await?,
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
		rs: RecordStrategy,
	) -> Result<Value> {
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

		// Extract the expected behaviour depending on the presence of EXPLAIN with or
		// without FULL
		let mut plan = Plan::new(ctx, stm, &self.entries, &self.results);
		// Check if we actually need to process and iterate over the results
		if plan.do_iterate {
			if let Some(e) = &mut plan.explanation {
				e.add_record_strategy(rs);
			}
			// Process prepared values
			let is_specific_permission = if let Some(qp) = ctx.get_query_planner() {
				let is_specific_permission = qp.is_any_specific_permission();
				while let Some(s) = qp.next_iteration_stage().await {
					let is_last = matches!(s, IterationStage::Iterate(_));
					let mut c = MutableContext::unfreeze(cancel_ctx)?;
					c.set_iteration_stage(s);
					cancel_ctx = c.freeze();
					if !is_last {
						self.clone()
							.iterate(stk, &cancel_ctx, opt, stm, is_specific_permission, None)
							.await?;
					};
				}
				is_specific_permission
			} else {
				false
			};
			// Process all documents
			self.iterate(
				stk,
				&cancel_ctx,
				opt,
				stm,
				is_specific_permission,
				plan.explanation.as_mut(),
			)
			.await?;
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
					self.iterate(stk, ctx, opt, stm, is_specific_permission, None).await?;
				}
			}
			// Process any SPLIT AT clause
			self.output_split(stk, ctx, opt, stm, rs).await?;
			// Process any GROUP BY clause
			self.output_group(stk, ctx, opt, stm).await?;
			// Process any ORDER BY clause
			if let Some(orders) = stm.order() {
				#[cfg(not(target_family = "wasm"))]
				self.results.sort(orders).await?;
				#[cfg(target_family = "wasm")]
				self.results.sort(orders);
			}
			// Process any START & LIMIT clause
			self.results.start_limit(self.start_skip, self.start, self.limit).await?;
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
	) -> Result<()> {
		if self.limit.is_none() {
			if let Some(v) = stm.limit() {
				self.limit = Some(v.process(stk, ctx, opt, None).await?);
			}
		}
		Ok(())
	}

	#[inline]
	pub(crate) fn is_limit_one_or_zero(&self) -> bool {
		self.limit.map(|v| v <= 1).unwrap_or(false)
	}

	#[inline]
	async fn setup_start(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<()> {
		if let Some(v) = stm.start() {
			self.start = Some(v.process(stk, ctx, opt, None).await?);
		}
		Ok(())
	}

	/// Determines whether START/LIMIT clauses can be optimized at the storage
	/// level.
	///
	/// This method enables a critical performance optimization where START and
	/// LIMIT clauses can be applied directly at the storage/iterator level
	/// (using `start_skip` and `cancel_on_limit`) rather than after all query
	/// processing is complete.
	///
	/// ## The Optimization
	///
	/// When this method returns `true`, the query engine can:
	/// - Skip records at the storage level before any processing (`start_skip`)
	/// - Cancel iteration early when the limit is reached (`cancel_on_limit`)
	///
	/// This provides significant performance benefits for queries with large
	/// result sets, as it avoids unnecessary processing of records that would
	/// be filtered out anyway.
	///
	/// ## Safety Conditions
	///
	/// The optimization is only safe when the order of records at the storage
	/// level matches the order of records in the final result set. This method
	/// returns `false` when any of the following conditions would change the
	/// record order or filtering:
	///
	/// ### GROUP BY clauses
	/// Grouping operations fundamentally change the result structure and record
	/// count, making storage-level limiting meaningless.
	///
	/// ### Multiple iterators
	/// When multiple iterators are involved (e.g., JOINs, UNIONs), records from
	/// different sources need to be merged, so individual iterator limits
	/// would be incorrect.
	///
	/// ### WHERE clauses
	/// Filtering operations change which records appear in the final result
	/// set. START should skip records from the filtered set, not from the raw
	/// storage.
	///
	/// Example problem:
	/// ```sql
	/// -- Given: t:1(f=true), t:2(f=true), t:3(f=false), t:4(f=false)
	/// SELECT * FROM t WHERE !f START 1;
	/// -- Correct: Skip first filtered record → [t:4]
	/// -- Wrong with optimization: Skip t:1 at storage, then filter → [t:3, t:4]
	/// ```
	///
	/// ### ORDER BY clauses (conditional)
	/// When there's an ORDER BY clause, the optimization is only safe if:
	/// - There's exactly one iterator
	/// - The iterator is backed by a sorted index
	/// - The index sort order matches the ORDER BY clause
	///
	/// ## Performance Impact
	///
	/// - **When enabled**: Significant performance improvement for large result sets
	/// - **When disabled**: Slight performance cost as all records must be processed before
	///   START/LIMIT is applied
	///
	/// ## Returns
	///
	/// - `true`: Safe to apply START/LIMIT optimization at storage level
	/// - `false`: Must apply START/LIMIT after all query processing is complete
	fn check_set_start_limit(&self, ctx: &Context, stm: &Statement<'_>) -> bool {
		// GROUP BY operations change the result structure and count, making
		// storage-level limiting meaningless
		if stm.group().is_some() {
			return false;
		}

		// Multiple iterators require merging records from different sources,
		// so individual iterator limits would be incorrect
		if self.entries.len() != 1 {
			return false;
		}

		// Check for WHERE clause
		if let Some(cond) = stm.cond() {
			// WHERE clauses filter records, so START should skip from the filtered set,
			// not from the raw storage. However, if there's exactly one index iterator
			// and the index is handling both the WHERE condition and ORDER BY clause,
			// then the optimization is safe because the index iterator is already
			// doing the appropriate filtering and ordering.
			if let Some(Iterable::Index(t, irf, _)) = self.entries.first() {
				if let Some(qp) = ctx.get_query_planner() {
					if let Some(exe) = qp.get_query_executor(t) {
						if exe.is_iterator_expression(*irf, &cond.0) {
							return true;
						}
					}
				}
			}
			return false;
		}

		// Without ORDER BY, the natural storage order is acceptable for START/LIMIT
		if stm.order().is_none() {
			return true;
		}

		// With ORDER BY, optimization is only safe if the iterator is backed by
		// a sorted index that matches the ORDER BY clause exactly
		if let Some(Iterable::Index(_, irf, _)) = self.entries.first() {
			if let Some(qp) = ctx.get_query_planner() {
				if qp.is_order(irf) {
					return true;
				}
			}
		}
		false
	}

	fn compute_start_limit(
		&mut self,
		ctx: &Context,
		stm: &Statement<'_>,
		is_specific_permission: bool,
	) {
		if self.check_set_start_limit(ctx, stm) {
			if let Some(l) = self.limit {
				// If we have a LIMIT, allow the collector to cancel the iteration once
				// this many items have been produced. This keeps long scans bounded.
				self.cancel_on_limit = Some(l);
			}
			// Only skip over the first N records (START/OFFSET) when there are no
			// specific per-record permission checks. When specific permissions are in play,
			// each record must be evaluated and cannot be blindly skipped.
			if !is_specific_permission {
				let s = self.start.unwrap_or(0) as usize;
				if s > 0 {
					self.start_skip = Some(s);
				}
			}
		}
	}

	pub(super) fn start_limit(&self) -> Option<&u32> {
		self.cancel_on_limit.as_ref()
	}

	/// Return the number of record that should be skipped
	pub(super) fn skippable(&self) -> usize {
		self.start_skip.unwrap_or(0)
	}

	/// Confirm the number of records that have been skipped
	pub(super) fn skipped(&mut self, skipped: usize) {
		if let Some(s) = &mut self.start_skip {
			*s -= skipped;
		}
	}

	async fn output_split(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		rs: RecordStrategy,
	) -> Result<()> {
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
								self.results.push(stk, ctx, opt, stm, rs, obj).await?;
							}
						}
						_ => {
							// Make a copy of object
							let mut obj = obj.clone();
							// Set the value at the path
							obj.set(stk, ctx, opt, split, val).await?;
							// Add the object to the results
							self.results.push(stk, ctx, opt, stm, rs, obj).await?;
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
	) -> Result<()> {
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
	) -> Result<()> {
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

	async fn iterate(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		is_specific_permission: bool,
		exp: Option<&mut Explanation>,
	) -> Result<()> {
		// Compute iteration limits
		self.compute_start_limit(ctx, stm, is_specific_permission);
		if let Some(e) = exp {
			if self.start_skip.is_some() || self.cancel_on_limit.is_some() {
				e.add_start_limit(self.start_skip, self.cancel_on_limit);
			}
		}
		// Prevent deep recursion
		let opt = opt.dive(4)?;
		// If any iterator requires distinct, we need to create a global distinct
		// instance
		let mut distinct = SyncDistinct::new(ctx);
		// Process all prepared values
		for (count, v) in mem::take(&mut self.entries).into_iter().enumerate() {
			v.iterate(stk, ctx, &opt, stm, self, distinct.as_mut()).await?;
			// MOCK can create a large collection of iterators,
			// we need to make space for possible cancellations
			if ctx.is_done(count % 100 == 0).await? {
				break;
			}
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
	) -> Result<()> {
		let rs = pro.rs;
		// Extract the value
		let res = Self::extract_value(stk, ctx, opt, stm, pro).await;
		// Process the result
		self.result(stk, ctx, opt, stm, rs, res).await;
		// Everything ok
		Ok(())
	}

	async fn extract_value(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		pro: Processed,
	) -> Result<Value, IgnoreError> {
		// Check if this is a count all
		let count_all = stm.expr().is_some_and(Fields::is_count_all_only);
		if count_all {
			if let Operable::Count(count) = pro.val {
				return Ok(count.into());
			}
			if matches!(pro.rs, RecordStrategy::KeysOnly) {
				return Ok(map! { "count".to_string() => Value::from(1) }.into());
			}
		}
		// Otherwise, we process the document
		stk.run(|stk| Document::process(stk, ctx, opt, stm, pro)).await
	}

	/// Accept a processed record result
	async fn result(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
		rs: RecordStrategy,
		res: Result<Value, IgnoreError>,
	) {
		// yield
		yield_now!();
		// Process the result
		match res {
			Err(IgnoreError::Ignore) => {
				return;
			}
			Err(IgnoreError::Error(e)) => {
				self.error = Some(e);
				self.run.cancel();
				return;
			}
			Ok(v) => {
				if let Err(e) = self.results.push(stk, ctx, opt, stm, rs, v).await {
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
