use std::collections::BTreeSet;
use std::mem;
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::Record;
use crate::catalog::providers::TableProvider;
use crate::ctx::{Canceller, Context, FrozenContext};
use crate::dbs::distinct::SyncDistinct;
use crate::dbs::plan::{Explanation, Plan};
use crate::dbs::result::Results;
use crate::dbs::store::{MemoryOrdered, MemoryOrderedLimit, MemoryRandom};
use crate::dbs::{Options, Statement};
use crate::doc::{CursorDoc, Document, DocumentContext, IgnoreError, NsDbCtx, NsDbTbCtx};
use crate::err::Error;
use crate::expr::lookup::{ComputedLookupSubject, LookupKind};
use crate::expr::order::Ordering;
use crate::expr::statements::relate::RelateThrough;
use crate::expr::{self, ControlFlow, Expr, Fields, FlowResultExt, Literal, Lookup, Mock, Part};
use crate::idx::planner::iterators::{IteratorRecord, IteratorRef};
use crate::idx::planner::{
	GrantedPermission, IterationStage, QueryPlanner, RecordStrategy, ScanDirection,
	StatementContext,
};
use crate::val::{RecordId, RecordIdKey, RecordIdKeyRange, TableName, Value};

const TARGET: &str = "surrealdb::core::dbs";

#[derive(Clone, Debug)]
pub(crate) enum Iterable {
	/// Any [Value] which does not exist in storage. This
	/// could be the result of a query, an arbitrary
	/// SurrealQL value, object, or array of values.
	Value(NsDbCtx, Value),
	/// An iterable which does not actually fetch the record
	/// data from storage. This is used in CREATE statements
	/// where we attempt to write data without first checking
	/// if the record exists, throwing an error on failure.
	Defer(NsDbTbCtx, RecordId),
	/// An iterable whose Record ID needs to be generated
	/// before processing. This is used in CREATE statements
	/// when generating a new id, or generating an id based
	/// on the id field which is specified within the data.
	GenerateRecordId(NsDbTbCtx, TableName),
	/// An iterable which needs to fetch the data of a
	/// specific record before processing the document.
	RecordId(NsDbTbCtx, RecordId),
	/// An iterable which needs to fetch the related edges
	/// of a record before processing each document.
	Lookup {
		doc_ctx: NsDbTbCtx,
		kind: LookupKind,
		from: RecordId,
		what: Vec<ComputedLookupSubject>,
	},
	/// An iterable which needs to iterate over the records
	/// in a table before processing each document.
	Table(NsDbTbCtx, TableName, RecordStrategy, ScanDirection),
	/// An iterable which fetches a specific range of records
	/// from storage, used in range and time-series scenarios.
	Range(NsDbTbCtx, TableName, RecordIdKeyRange, RecordStrategy, ScanDirection),
	/// An iterable which fetches a record from storage, and
	/// which has the specific value to update the record with.
	/// This is used in INSERT statements, where each value
	/// passed in to the iterable is unique for each record.
	/// This tuples takes in:
	/// - The table name
	/// - The optional id key. When none is provided, it will be generated at a later stage and no
	///   record fetch will be done. This can be NONE in a scenario like: `INSERT INTO test {
	///   there_is: 'no id set' }`
	/// - The value for the record
	Mergeable(NsDbTbCtx, TableName, Option<RecordIdKey>, Value),
	/// An iterable which fetches a record from storage, and
	/// which has the specific value to update the record with.
	/// This is used in RELATE statements. The optional value
	/// is used in INSERT RELATION statements, where each value
	/// passed in to the iterable is unique for each record.
	///
	/// The first field is the rid from which we create, the second is the rid
	/// which is the relation itself and the third is the target of the
	/// relation
	Relatable(NsDbTbCtx, RecordId, RelateThrough, RecordId, Option<Value>),
	/// An iterable which iterates over an index range for a
	/// table, which then fetches the corresponding records
	/// which are matched within the index.
	/// When the 3rd argument is true, we iterate over keys only.
	Index(NsDbTbCtx, TableName, IteratorRef, RecordStrategy),
}

/// Operable
#[derive(Debug)]
pub(crate) enum Operable {
	/// CREATE person CONTENT { name: 'John Doe' }
	Value(Arc<Record>),
	/// Record is the record we're operating on (eg. )
	/// Second argument is `ON DUPLICATE KEY` value.
	Insert(Arc<Record>, Arc<Value>),
	/// 1. RecordId
	/// 2. Record
	/// 3. Relation RecordId
	/// 4. For update operations if it doesn't exist (TODO: This may be true, maybe not)
	Relate(RecordId, Arc<Record>, RecordId, Option<Arc<Value>>),

	Count(usize),
}

/// Workable is used in the Document to get additional information specific to an insert statement
/// or relate statement.
#[derive(Debug)]
pub(crate) enum Workable {
	Normal,
	Insert(Arc<Value>),
	Relate(RecordId, RecordId, Option<Arc<Value>>),
}

#[derive(Debug)]
pub(crate) struct Processable {
	/// The document context for this document
	pub(crate) doc_ctx: DocumentContext,
	/// Whether this document only fetched keys or just count
	pub(crate) record_strategy: RecordStrategy,
	/// Whether this document needs to have an ID generated
	pub(crate) generate: Option<TableName>,
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
	canceller: Canceller,
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
	/// Precomputed number of accepted results after which we can stop iterating early.
	/// - When storage-level START skip is active (`start_skip.is_some()`), this is just `limit`.
	/// - Otherwise, we must collect `start + limit` results so that the final START can be applied
	///   in post-processing without starving the LIMIT.
	cancel_threshold: Option<usize>,
}

impl Clone for Iterator {
	fn clone(&self) -> Self {
		Self {
			canceller: self.canceller.clone(),
			limit: self.limit,
			start: self.start,
			start_skip: self.start_skip.map(|_| self.start.unwrap_or(0) as usize),
			error: None,
			results: Results::default(),
			entries: self.entries.clone(),
			guaranteed: None,
			cancel_on_limit: None,
			cancel_threshold: None,
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
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
		planner: &mut QueryPlanner,
		stm_ctx: &StatementContext<'_>,
		doc_ctx: &NsDbCtx,
		val: &Expr,
	) -> Result<()> {
		// Match the values
		match val {
			Expr::Mock(mock) => self.prepare_mock(ctx, opt, stm_ctx, doc_ctx, mock).await?,
			Expr::Table(table_name) => {
				self.prepare_table(ctx, opt, stk, planner, stm_ctx, doc_ctx, table_name).await?
			}
			Expr::Idiom(x) => {
				// TODO: This needs to be structured better.
				// match against what previously would be an edge.
				if x.len() != 2 {
					return self
						.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, doc_ctx, val)
						.await;
				}

				let Part::Start(Expr::Literal(Literal::RecordId(ref from))) = x[0] else {
					return self
						.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, doc_ctx, val)
						.await;
				};

				let Part::Lookup(ref lookup) = x[1] else {
					return self
						.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, doc_ctx, val)
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
					// TODO: Do we support `RETURN a:b` here? What do we do when it is not of the
					// right type?
					let from = match from.compute(stk, ctx, opt, doc).await {
						Ok(x) => x,
						Err(ControlFlow::Err(e)) => return Err(e),
						Err(_) => bail!(Error::InvalidControlFlow),
					};
					let mut what = Vec::new();
					for s in lookup.what.iter() {
						what.push(s.compute(stk, ctx, opt, doc).await?);
					}
					// idiom matches the Edges pattern.
					self.prepare_lookup(
						ctx,
						opt,
						stm_ctx.stm,
						doc_ctx,
						from,
						lookup.kind.clone(),
						what,
					)
					.await?;
				}
			}
			Expr::Literal(Literal::Array(array)) => {
				self.prepare_array(stk, ctx, opt, doc, planner, stm_ctx, doc_ctx, array).await?
			}
			x => self.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, doc_ctx, x).await?,
		};
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn prepare_table(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		stk: &mut Stk,
		planner: &mut QueryPlanner,
		stm_ctx: &StatementContext<'_>,
		doc_ctx: &NsDbCtx,
		table: &TableName,
	) -> Result<()> {
		let tb = if stm_ctx.stm.requires_table_existence() {
			ctx.tx().expect_tb(doc_ctx.ns.namespace_id, doc_ctx.db.database_id, table).await?
		} else {
			ctx.tx().get_or_add_tb(Some(ctx), &doc_ctx.ns.name, &doc_ctx.db.name, table).await?
		};

		let fields = ctx
			.tx()
			.all_tb_fields(doc_ctx.ns.namespace_id, doc_ctx.db.database_id, table, opt.version)
			.await?;
		let doc_ctx = NsDbTbCtx {
			ns: Arc::clone(&doc_ctx.ns),
			db: Arc::clone(&doc_ctx.db),
			tb,
			fields,
		};

		// We add the iterable only if we have a permission
		let granted_perms = planner.check_table_permission(stm_ctx, table).await?;
		if matches!(granted_perms, GrantedPermission::None) {
			return Ok(());
		}
		// Add the record to the iterator
		if stm_ctx.stm.is_deferable() {
			self.ingest(Iterable::GenerateRecordId(doc_ctx, table.clone()));
			return Ok(());
		}

		if stm_ctx.stm.is_guaranteed() {
			self.guaranteed = Some(Iterable::GenerateRecordId(doc_ctx.clone(), table.clone()));
		}

		planner.add_iterables(stk, stm_ctx, doc_ctx, table, granted_perms, self).await?;

		Ok(())
	}

	/// Prepares a RecordId for processing
	pub(crate) async fn prepare_record_id(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		planner: &mut QueryPlanner,
		stm_ctx: &StatementContext<'_>,
		doc_ctx: &NsDbCtx,
		rid: RecordId,
	) -> Result<()> {
		let tb = if stm_ctx.stm.requires_table_existence() {
			ctx.tx().expect_tb(doc_ctx.ns.namespace_id, doc_ctx.db.database_id, &rid.table).await?
		} else {
			ctx.tx()
				.get_or_add_tb(Some(ctx), &doc_ctx.ns.name, &doc_ctx.db.name, &rid.table)
				.await?
		};
		let fields = ctx
			.tx()
			.all_tb_fields(doc_ctx.ns.namespace_id, doc_ctx.db.database_id, &rid.table, opt.version)
			.await?;

		let doc_ctx = NsDbTbCtx {
			ns: Arc::clone(&doc_ctx.ns),
			db: Arc::clone(&doc_ctx.db),
			tb,
			fields,
		};

		if rid.key.is_range() {
			return self.prepare_range(planner, stm_ctx, doc_ctx, rid).await;
		}
		// We add the iterable only if we have a permission
		if matches!(
			planner.check_table_permission(stm_ctx, &rid.table).await?,
			GrantedPermission::None
		) {
			return Ok(());
		}
		// Add the record to the iterator
		match stm_ctx.stm.is_deferable() {
			true => self.ingest(Iterable::Defer(doc_ctx, rid)),
			false => self.ingest(Iterable::RecordId(doc_ctx, rid)),
		}
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	pub(crate) async fn prepare_mock(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		stm_ctx: &StatementContext<'_>,
		doc_ctx: &NsDbCtx,
		mock: &Mock,
	) -> Result<()> {
		ensure!(!stm_ctx.stm.is_only() || self.is_limit_one_or_zero(), Error::SingleOnlyOutput);

		// For deferable statements (CREATE, UPSERT without condition), auto-create the table
		let tb = if stm_ctx.stm.is_deferable() {
			ctx.tx()
				.get_or_add_tb(Some(ctx), &doc_ctx.ns.name, &doc_ctx.db.name, mock.table())
				.await?
		} else {
			ctx.tx()
				.expect_tb(doc_ctx.ns.namespace_id, doc_ctx.db.database_id, mock.table())
				.await?
		};
		let fields = ctx
			.tx()
			.all_tb_fields(
				doc_ctx.ns.namespace_id,
				doc_ctx.db.database_id,
				mock.table(),
				opt.version,
			)
			.await?;
		let doc_ctx = NsDbTbCtx {
			ns: Arc::clone(&doc_ctx.ns),
			db: Arc::clone(&doc_ctx.db),
			tb,
			fields,
		};

		// Add the records to the iterator
		for (count, rid) in mock.clone().into_iter().enumerate() {
			if stm_ctx.stm.is_deferable() {
				self.ingest(Iterable::Defer(doc_ctx.clone(), rid))
			} else {
				self.ingest(Iterable::RecordId(doc_ctx.clone(), rid))
			}
			// Check if the context is finished
			if stm_ctx.ctx.is_done(Some(count)).await? {
				break;
			}
		}
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn prepare_lookup(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
		doc_ctx: &NsDbCtx,
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
			.to_sql();

			bail!(Error::InvalidStatementTarget {
				value,
			})
		}

		let txn = ctx.tx();
		let tb = if stm.requires_table_existence() {
			txn.expect_tb(doc_ctx.ns.namespace_id, doc_ctx.db.database_id, &from.table).await?
		} else {
			txn.get_or_add_tb(Some(ctx), &doc_ctx.ns.name, &doc_ctx.db.name, &from.table).await?
		};
		let fields = txn
			.all_tb_fields(
				doc_ctx.ns.namespace_id,
				doc_ctx.db.database_id,
				&from.table,
				opt.version,
			)
			.await?;

		let doc_ctx = NsDbTbCtx {
			ns: Arc::clone(&doc_ctx.ns),
			db: Arc::clone(&doc_ctx.db),
			tb,
			fields,
		};

		// Add the record to the iterator
		self.ingest(Iterable::Lookup {
			doc_ctx,
			from,
			kind,
			what,
		});
		// All ingested ok
		Ok(())
	}

	/// Prepares a value for processing
	pub(crate) async fn prepare_range(
		&mut self,
		planner: &mut QueryPlanner,
		stm_ctx: &StatementContext<'_>,
		doc_ctx: NsDbTbCtx,
		rid: RecordId,
	) -> Result<()> {
		// We add the iterable only if we have a permission
		let p = planner.check_table_permission(stm_ctx, &rid.table).await?;
		if matches!(p, GrantedPermission::None) {
			return Ok(());
		}
		// Check if this is a create statement
		ensure!(
			!stm_ctx.stm.is_create(),
			Error::InvalidStatementTarget {
				value: rid.to_sql(),
			}
		);
		// Evaluate if we can only scan keys (rather than keys AND values), or count
		let rs = stm_ctx.check_record_strategy(false, p)?;
		let sc = stm_ctx.check_scan_direction();
		// Add the record to the iterator
		if let (tb, RecordIdKey::Range(v)) = (rid.table, rid.key) {
			self.ingest(Iterable::Range(doc_ctx, tb, *v, rs, sc));
		}
		// All ingested ok
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn prepare_computed(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
		planner: &mut QueryPlanner,
		stm_ctx: &StatementContext<'_>,
		doc_ctx: &NsDbCtx,
		expr: &Expr,
	) -> Result<()> {
		let v = stk.run(|stk| expr.compute(stk, ctx, opt, doc)).await.catch_return()?;
		match v {
			Value::Table(table_name) => {
				self.prepare_table(ctx, opt, stk, planner, stm_ctx, doc_ctx, &table_name).await?
			}
			Value::RecordId(rid) => {
				self.prepare_record_id(ctx, opt, planner, stm_ctx, doc_ctx, rid).await?
			}
			Value::Array(array) => {
				for v in array {
					match v {
						Value::Table(table) => {
							self.prepare_table(ctx, opt, stk, planner, stm_ctx, doc_ctx, &table)
								.await?
						}
						Value::RecordId(rid) => {
							self.prepare_record_id(ctx, opt, planner, stm_ctx, doc_ctx, rid).await?
						}
						v if stm_ctx.stm.is_select() => {
							self.ingest(Iterable::Value(doc_ctx.clone(), v))
						}
						Value::Object(o) => {
							if let Some(id) = o.rid() {
								self.prepare_record_id(ctx, opt, planner, stm_ctx, doc_ctx, id)
									.await?;
							} else {
								bail!(Error::InvalidStatementTarget {
									value: Value::Object(o).to_sql(),
								})
							}
						}
						v => {
							bail!(Error::InvalidStatementTarget {
								value: v.to_sql(),
							})
						}
					}
				}
			}
			v if stm_ctx.stm.is_select() => self.ingest(Iterable::Value(doc_ctx.clone(), v)),
			Value::Object(o) => {
				if let Some(id) = o.rid() {
					self.prepare_record_id(ctx, opt, planner, stm_ctx, doc_ctx, id).await?;
				} else {
					bail!(Error::InvalidStatementTarget {
						value: o.to_sql(),
					})
				}
			}
			v => {
				bail!(Error::InvalidStatementTarget {
					value: v.to_sql(),
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
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
		planner: &mut QueryPlanner,
		stm_ctx: &StatementContext<'_>,
		doc_ctx: &NsDbCtx,
		v: &[Expr],
	) -> Result<()> {
		ensure!(!stm_ctx.stm.is_only() || self.is_limit_one_or_zero(), Error::SingleOnlyOutput);
		// Add the records to the iterator
		for v in v {
			match v {
				Expr::Mock(v) => self.prepare_mock(ctx, opt, stm_ctx, doc_ctx, v).await?,
				Expr::Table(table_name) => {
					self.prepare_table(ctx, opt, stk, planner, stm_ctx, doc_ctx, table_name).await?
				}
				Expr::Idiom(x) => {
					// match against what previously would be an edge.
					if x.len() != 2 {
						return self
							.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, doc_ctx, v)
							.await;
					}

					let Part::Start(Expr::Literal(Literal::RecordId(ref from))) = x[0] else {
						return self
							.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, doc_ctx, v)
							.await;
					};

					let Part::Lookup(ref lookup) = x[0] else {
						return self
							.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, doc_ctx, v)
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
							what.push(s.compute(stk, ctx, opt, doc).await?);
						}
						// idiom matches the Edges pattern.
						return self
							.prepare_lookup(
								ctx,
								opt,
								stm_ctx.stm,
								doc_ctx,
								from,
								lookup.kind.clone(),
								what,
							)
							.await;
					}

					self.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, doc_ctx, v).await?
				}
				v => {
					self.prepare_computed(stk, ctx, opt, doc, planner, stm_ctx, doc_ctx, v).await?
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
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
		rs: RecordStrategy,
	) -> Result<Value> {
		// Log the statement
		trace!(target: TARGET, statement = %stm.to_sql(), "Iterating statement");
		// Enable context override
		let mut cancel_ctx = Context::new(ctx);
		self.canceller = cancel_ctx.add_cancel();
		let mut cancel_ctx = cancel_ctx.freeze();
		// Process the query LIMIT clause
		self.setup_limit(stk, &cancel_ctx, opt, stm).await?;
		// Process the query START clause
		self.setup_start(stk, &cancel_ctx, opt, stm).await?;
		// Prepare the results with possible optimisations on groups
		self.results = self.results.prepare(ctx, stm, self.start, self.limit)?;

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
					let mut c = Context::unfreeze(cancel_ctx)?;
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
			// NOTE: This is a no-op for order-less queries.
			self.results.sort().await?;
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
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<()> {
		if self.limit.is_none()
			&& let Some(v) = stm.limit()
		{
			self.limit = Some(v.process(stk, ctx, opt, None).await?);
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
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<()> {
		if let Some(v) = stm.start() {
			self.start = Some(v.process(stk, ctx, opt, None).await?);
		}
		Ok(())
	}

	/// Returns true if START can be applied as a storage-level skip (start_skip)
	/// without changing the semantics of the query.
	///
	/// What this actually checks (mirrors the code below):
	/// - GROUP BY: disallowed, because grouping changes the result count/order. → false
	/// - Multiple iterators: disallowed, because START must apply to the merged set. → false
	/// - WHERE: allowed only if the sole iterator is an index whose executor applies the WHERE
	///   predicate at the iterator level (`exe.is_iterator_condition`). Otherwise, START must apply
	///   to the filtered set and cannot be pushed down. → conditional
	/// - ORDER BY absent: allowed, natural storage order is fine. → true
	/// - ORDER BY present: allowed only if the sole iterator is a sorted index that provides the
	///   requested order (`qp.is_order`). → conditional
	///
	/// In short: push START down to storage only for a single iterator where any filtering is
	/// performed by the index itself and, if ORDER BY is used, the iterator natively yields
	/// rows in the required order.
	fn can_start_skip(&self, ctx: &FrozenContext, stm: &Statement<'_>) -> bool {
		// GROUP BY operations change the result structure and count
		if stm.group().is_some() {
			return false;
		}
		// Only safe when a single iterator is used
		if self.entries.len() != 1 {
			return false;
		}
		// START must apply to the filtered set. Therefore, disallow with WHERE
		// unless the iterator itself applies the condition (index executor).
		if let Some(cond) = stm.cond() {
			if let Some(Iterable::Index(_doc_ctx, t, irf, _)) = self.entries.first() {
				if let Some(qp) = ctx.get_query_planner() {
					if let Some(exe) = qp.get_query_executor(t) {
						if exe.is_iterator_expression(*irf, &cond.0) {
							// Allowed: index handles the filtering
						} else {
							return false;
						}
					} else {
						return false;
					}
				} else {
					return false;
				}
			} else {
				// WHERE exists but iterator is not an index -> cannot start-skip
				return false;
			}
		}
		// Without ORDER BY, natural order is acceptable
		if stm.order().is_none() {
			return true;
		}
		// With ORDER BY, only safe if iterator is a sorted index matching ORDER
		if let Some(Iterable::Index(_doc_ctx, _, irf, _)) = self.entries.first()
			&& let Some(qp) = ctx.get_query_planner()
			&& qp.is_order(irf)
		{
			return true;
		}
		false
	}

	/// Returns true if iteration can be cancelled early on LIMIT (cancel_on_limit)
	/// without changing the semantics of the query.
	///
	/// What this actually checks (mirrors the code below):
	/// - GROUP BY: disallowed; grouping can change the number of output rows and needs to see all
	///   inputs. → false
	/// - ORDER BY absent: allowed; we count accepted rows after WHERE filtering, so we can stop as
	///   soon as we have enough outputs. → true
	/// - ORDER BY present: allowed only if there's exactly one iterator and it is a sorted index
	///   that matches the ORDER BY (`qp.is_order`). Otherwise we must iterate all rows to sort
	///   correctly. → conditional
	///
	/// Note: WHERE filtering is fine here because cancellation is based on the number of
	/// accepted results after filtering, not on the raw scanned rows.
	fn can_cancel_on_limit(&self, ctx: &FrozenContext, stm: &Statement<'_>) -> bool {
		// GROUP BY changes result count post-iteration.
		// Cannot cancel early as we need to evalute every records.
		if stm.group().is_some() {
			return false;
		}
		// WHERE is allowed: we count accepted results after filtering
		// ORDER requires special handling
		if stm.order().is_none() {
			return true;
		}
		// With ORDER BY, only safe if the only iterator is backed by a sorted index
		if self.entries.len() == 1
			&& let Some(Iterable::Index(_doc_ctx, _, irf, _)) = self.entries.first()
			&& let Some(qp) = ctx.get_query_planner()
			&& qp.is_order(irf)
		{
			return true;
		}
		false
	}

	fn compute_start_limit(
		&mut self,
		ctx: &FrozenContext,
		stm: &Statement<'_>,
		is_specific_permission: bool,
	) {
		// Determine if we can skip records at the storage level for START
		if !is_specific_permission && self.can_start_skip(ctx, stm) {
			let s = self.start.unwrap_or(0) as usize;
			if s > 0 {
				self.start_skip = Some(s);
			}
		}
		// Determine if we can stop iteration early once enough results are accepted
		//
		// We precompute a single cancellation threshold because the condition that
		// influences it (whether START is applied at storage-level via `start_skip`)
		// is fixed for the whole iteration. Even though `start_skip`'s internal
		// counter is decremented during scanning, the fact that the initial START
		// was applied at the storage level does not change — therefore the threshold
		// does not need to be recomputed per result.
		if self.can_cancel_on_limit(ctx, stm)
			&& let Some(l) = self.limit
		{
			self.cancel_on_limit = Some(l);
			if self.start_skip.is_some() {
				// START is applied by the storage iterator. We are only collecting
				// post-START results, so we can cancel as soon as we accepted `limit`.
				self.cancel_threshold = Some(l as usize)
			} else {
				// START cannot be applied by the storage iterator. We must accumulate
				// enough accepted results to later drop `start` of them during
				// post-processing and still return `limit` items. Hence `start + limit`.
				self.cancel_threshold = Some((l + self.start.unwrap_or(0)) as usize);
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
		ctx: &FrozenContext,
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
								self.results.push(stk, ctx, opt, rs, obj).await?;
							}
						}
						Value::Set(v) => {
							for val in v {
								// Make a copy of object
								let mut obj = obj.clone();
								// Set the value at the path
								obj.set(stk, ctx, opt, split, val).await?;
								// Add the object to the results
								self.results.push(stk, ctx, opt, rs, obj).await?;
							}
						}
						_ => {
							// Make a copy of object
							let mut obj = obj.clone();
							// Set the value at the path
							obj.set(stk, ctx, opt, split, val).await?;
							// Add the object to the results
							self.results.push(stk, ctx, opt, rs, obj).await?;
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
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<()> {
		// Process any GROUP clause
		if let Results::Groups(g) = &mut self.results {
			// Get the grouped values from the collector
			let mut collector = g.output(stk, ctx, opt).await?;
			let values = collector.take_vec();

			// Create the appropriate Results variant based on ORDER BY clause
			self.results = if let Some(ordering) = stm.order() {
				match ordering {
					Ordering::Random => {
						let mut res = MemoryRandom::new(None);
						for val in values {
							res.push(val);
						}
						Results::MemoryRandom(res)
					}
					Ordering::Order(orders) => {
						// Check if we should use the priority queue optimization
						if let Some(limit) = self.limit {
							let effective_limit = self.start.unwrap_or(0) + limit;
							if effective_limit
								<= ctx.config().limits.max_order_limit_priority_queue_size
							{
								let mut res = MemoryOrderedLimit::new(
									effective_limit as usize,
									orders.clone(),
								);
								for val in values {
									res.push(val);
								}
								Results::MemoryOrderedLimit(res)
							} else {
								// Use standard MemoryOrdered
								let mut res = MemoryOrdered::new(orders.clone(), None);
								for val in values {
									res.push(val);
								}
								Results::MemoryOrdered(res)
							}
						} else {
							// No limit, use standard MemoryOrdered
							let mut res = MemoryOrdered::new(orders.clone(), None);
							for val in values {
								res.push(val);
							}
							Results::MemoryOrdered(res)
						}
					}
				}
			} else {
				// No ORDER BY, just use Memory
				Results::Memory(values.into())
			};
		}
		// Everything ok
		Ok(())
	}

	async fn output_fetch(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<()> {
		if let Some(fetchs) = stm.fetch() {
			let mut idioms = BTreeSet::new();
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
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
		is_specific_permission: bool,
		exp: Option<&mut Explanation>,
	) -> Result<()> {
		// Compute iteration limits
		self.compute_start_limit(ctx, stm, is_specific_permission);
		if let Some(e) = exp
			&& (self.start_skip.is_some() || self.cancel_on_limit.is_some())
		{
			e.add_start_limit(self.start_skip, self.cancel_on_limit);
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
			if ctx.is_done(Some(count)).await? {
				break;
			}
		}
		// Everything processed ok
		Ok(())
	}

	/// Process a new record Thing and Value
	#[instrument(level = "trace", name = "Iterator::process", skip_all)]
	pub async fn process(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
		pro: Processable,
	) -> Result<()> {
		let rs = pro.record_strategy;
		// Extract the value
		let res = Self::extract_value(stk, ctx, opt, stm, pro).await;
		// Process the result
		self.result(stk, ctx, opt, rs, res).await;
		// Everything ok
		Ok(())
	}

	#[instrument(level = "trace", name = "Iterator::extract_value", skip_all)]
	async fn extract_value(
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
		pro: Processable,
	) -> Result<Value, IgnoreError> {
		// Check if this is a count all
		let count_all = stm.expr().is_some_and(Fields::is_count_all_only);
		if count_all {
			if let Operable::Count(count) = pro.val {
				return Ok(count.into());
			}
			if matches!(pro.record_strategy, RecordStrategy::KeysOnly) {
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
		ctx: &FrozenContext,
		opt: &Options,
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
				self.canceller.cancel();
				return;
			}
			Ok(v) => {
				if let Err(e) = self.results.push(stk, ctx, opt, rs, v).await {
					self.error = Some(e);
					self.canceller.cancel();
					return;
				}
			}
		}
		// Check if we have collected enough accepted results to stop.
		// We use equality here because results are appended one-by-one; once the
		// threshold is reached, further work would be wasted as START/LIMIT
		// post-processing (if any) already has enough input to produce the final output.
		if let Some(cancel_threshold) = self.cancel_threshold
			&& self.results.len() == cancel_threshold
		{
			self.canceller.cancel()
		}
	}
}
