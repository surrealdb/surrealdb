//! The foreign views a write maintains.
//!
//! One stage with three arms, because a
//! `DEFINE TABLE … AS SELECT` produces three different obligations:
//!
//! - a view nothing maintains, which a write does nothing for;
//! - a **materialized** view, which holds one row per source record, under that record's own key;
//! - an **aggregated** view, which holds one row per group, whose stored stats a write adjusts by
//!   the delta the record made rather than by recomputing the group.
//!
//! # A view row is a record
//!
//! Both maintained arms end the same way: the row they wrote or deleted is
//! driven through [the view-maintenance
//! order](super::super::plan::WritePlan::view_maintenance), which is the view
//! table's own indexes, views, events, subscriptions and change feed. A view
//! table is a real table — a second-order view can read it, a subscription can
//! watch it, an index can cover it — and `DEFINE TABLE … AS SELECT` already
//! runs that lifecycle over every row of its initial build.
//!
//! # The frame
//!
//! Every clause here runs with permission enforcement off and writes allowed,
//! under the *writer's* authority: a view definition records no `AUTH LIMIT`,
//! so there is nothing to narrow to. See
//! [`view_frame`](crate::exec::permission::view_frame).

use std::borrow::Cow;
use std::sync::Arc;

use surrealdb_types::ToSql;

use crate::catalog::aggregation::{
	self, AggregationAnalysis, AggregationStat, Recalculation, add_to_aggregation_stats,
	apply_recalculations, remove_from_aggregation_stats, update_aggregation_stats,
};
use crate::catalog::providers::TableProvider;
use crate::catalog::{Metadata, Record, RecordType, TableDefinition, ViewDefinition};
use crate::doc::Action;
use crate::exe::FlowResultExt;
use crate::exec::operators::write::apply;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::row::WriteRow;
use crate::exec::operators::write::scope::{NestedKey, WriteScope, run_nested};
use crate::exec::operators::write::stage::{StageId, StageOutcome, WriteStage};
use crate::exec::permission::{
	PhysicalAggregatedView, PhysicalView, PhysicalViewKind, view_frame, view_physical,
};
use crate::exec::{BoxFut, Error as ExecError, EvalContext, ExecutionContext, PhysicalExpr};
use crate::expr::field::Selector;
use crate::expr::logical::LogicalOp;
use crate::expr::statements::SelectStatement;
use crate::expr::{
	BinaryOperator, Cond, ControlFlow, Expr, Fields, Function, FunctionCall, Groups, Literal, Param,
};
use crate::key::schema::RecordKey;
use crate::val::{Array, RecordId, RecordIdKey, TableName, Value};

/// This module's stages, by id.
#[allow(
	clippy::wildcard_enum_match_arm,
	reason = "a stage this module does not own is not this module's to name, and `super::lookup` \
	          routes none of them here"
)]
pub(crate) fn stage(id: StageId) -> Option<Arc<dyn WriteStage>> {
	match id {
		StageId::ProcessTableViews(action) => Some(Arc::new(ProcessTableViews(action))),
		// `super::lookup` routes only this module's stages here, so anything
		// else is a routing mistake rather than a missing body.
		_ => None,
	}
}

/// Maintain every foreign view this table feeds.
///
/// The first of the four observers on every order that has them, and — like
/// every observer — suppressed by an import and by a record whose post-image
/// equals its pre-image.
///
/// The payload is the action the order was assembled for, which decides which
/// image a view's condition is judged against and what a group's stats are
/// adjusted by.
#[derive(Debug)]
struct ProcessTableViews(Action);

impl WriteStage for ProcessTableViews {
	fn name(&self) -> &'static str {
		StageId::ProcessTableViews(self.0).name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			if ctx.views.is_empty() || !super::observe::is_observed(row, ctx) {
				return Ok(StageOutcome::Continue);
			}
			#[cfg(test)]
			VIEW_BODIES.with(|runs| runs.set(runs.get() + 1));
			// Read off the row before the first await: a `WriteRow` holds a
			// `OnceCell`, so a shared borrow alive across one would poison the
			// `Send` bound on every future this stage sits in.
			let Some(id) = row.id.clone() else {
				return Ok(StageOutcome::Continue);
			};
			let images = Images {
				initial: row.initial.clone(),
				current: row.current.clone(),
			};
			let scope = ctx.scope()?;
			let frame = view_frame(ctx.exec());

			for definition in ctx.views.iter() {
				match &*compiled_view(ctx, definition, &frame).await? {
					PhysicalViewKind::Unmaintained => {}
					PhysicalViewKind::Materialized(view) => {
						materialized(&scope, ctx, &frame, definition, view, &id, &images, self.0)
							.await?;
					}
					PhysicalViewKind::Aggregated(view) => {
						let ViewDefinition::Aggregated {
							analysis,
							tables,
							condition,
							..
						} = view_of(definition)?
						else {
							return Err(mismatched(&definition.name));
						};
						let target = Aggregated {
							view,
							sources: ViewSources {
								analysis,
								tables,
								condition: condition.as_ref(),
								table: &definition.name,
							},
						};
						aggregated(&scope, ctx, &frame, &target, &images, self.0).await?;
					}
				}
			}
			Ok(StageOutcome::Continue)
		})
	}
}

/// The two images a view's clauses are evaluated against, owned so that
/// nothing borrows the row across an await.
struct Images {
	/// The record as it stood before the write.
	initial: Value,
	/// The record as it stands after it.
	current: Value,
}

impl Images {
	/// The image a view judges, given the action: a delete is about what was
	/// there, everything else about what is.
	fn subject(&self, action: Action) -> &Value {
		match action {
			Action::Delete => &self.initial,
			Action::Create | Action::Update => &self.current,
		}
	}
}

#[cfg(test)]
thread_local! {
	/// How many times this thread ran the views stage's body over a record, for
	/// tests that must know a write maintained its views as instructions.
	pub(crate) static VIEW_BODIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// One aggregated view, with the pieces its maintenance reads.
struct Aggregated<'a> {
	/// The compiled clauses.
	view: &'a PhysicalAggregatedView,
	/// What the view is over, from its stored definition.
	sources: ViewSources<'a>,
}

/// What an aggregated view's stored definition says it is over: the pieces a
/// group's row and its recalculation read beside the view's clauses.
pub(crate) struct ViewSources<'a> {
	/// The stored analysis, which says how many stats a new group starts with
	/// and how the recalculation names them.
	pub(crate) analysis: &'a AggregationAnalysis,
	/// The tables the view selects from, which a recalculation re-reads.
	pub(crate) tables: &'a [TableName],
	/// The view's condition in its stored form, which a recalculation ANDs
	/// into its own.
	pub(crate) condition: Option<&'a Expr>,
	/// The view table.
	pub(crate) table: &'a TableName,
}

/// The clauses that maintain one foreign view, compiled on first use and once
/// for the statement.
async fn compiled_view(
	ctx: &WriteCtx,
	definition: &TableDefinition,
	frame: &ExecutionContext,
) -> Result<Arc<PhysicalViewKind>, ControlFlow> {
	{
		let compiled = ctx.views_compiled.lock().await;
		if let Some((_, view)) = compiled.iter().find(|(name, _)| *name == definition.name) {
			return Ok(Arc::clone(view));
		}
	}
	let view = Arc::new(
		crate::exec::vm::descriptor::write_unit(async {
			let planner = crate::exec::operators::write::scope::planner_for(frame)?;
			view_physical(view_of(definition)?, &planner)
				.await
				.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))
		})
		.await?,
	);
	let mut compiled = ctx.views_compiled.lock().await;
	if let Some((_, published)) = compiled.iter().find(|(name, _)| *name == definition.name) {
		return Ok(Arc::clone(published));
	}
	compiled.push((definition.name.clone(), Arc::clone(&view)));
	Ok(view)
}

/// The view a table stored as a view table holds.
fn view_of(definition: &TableDefinition) -> Result<&ViewDefinition, ControlFlow> {
	definition.view.as_ref().ok_or_else(|| mismatched(&definition.name))
}

/// A table the catalog lists as a view of this one, holding no view.
fn mismatched(table: &TableName) -> ControlFlow {
	ControlFlow::Err(anyhow::anyhow!(crate::err::EngineError::unreachable(format!(
		"the table {table} is stored as a view table but holds no view"
	))))
}

// ===========================================================================
// One row per record
// ===========================================================================

/// Maintain a materialized view: one row on the view table, under the source
/// record's own key.
///
/// The view's condition decides whether the record belongs in the view; a view
/// with none keeps every record the write did not delete. The row is written
/// or removed, and the view table's own lifecycle then runs over it — with the
/// action derived from what the view table held, not from what the statement
/// did to the source record: a record the condition newly admits *creates* a
/// view row even though the statement was an update.
#[allow(clippy::too_many_arguments)]
async fn materialized(
	scope: &Arc<WriteScope>,
	ctx: &WriteCtx,
	frame: &ExecutionContext,
	definition: &TableDefinition,
	view: &PhysicalView,
	id: &RecordId,
	images: &Images,
	action: Action,
) -> Result<(), ControlFlow> {
	let table = &definition.name;
	let key = id.key.clone();
	let subject = &images.current;
	let admitted = match &view.condition {
		Some(condition) => evaluate(condition, frame, subject).await?.is_truthy(),
		None => action != Action::Delete,
	};

	let ns = ctx.ns.namespace_id;
	let db = ctx.db.database_id;
	let txn = ctx.txn();
	// The row the view held, read before it is overwritten: it is the
	// before-image the view table's own lifecycle is shown, and whether it was
	// there decides whether this write creates or updates. Decoded raw, as
	// every read of a view row is, so the record's key is not spliced back
	// into its body.
	let before = read_view_row(ctx, table, &key).await?;

	let (view_action, after) = if admitted {
		let data = evaluate(&view.projection, frame, subject).await?;
		let record = Arc::new(Record::new(data));
		txn.set_record(ns, db, table, &key, Arc::clone(&record)).await.map_err(ControlFlow::Err)?;
		let action = match before {
			Some(_) => Action::Update,
			None => Action::Create,
		};
		(action, Some(record))
	} else {
		txn.del_record(ns, db, table, &key).await.map_err(ControlFlow::Err)?;
		// Removing a row the view never held changes nothing, so there is no
		// image for the view table's observers to be shown.
		if before.is_none() {
			return Ok(());
		}
		(Action::Delete, None)
	};

	let row = RecordId {
		table: table.clone(),
		key,
	};
	scope.run_view_row(table, row, view_action, before.as_deref(), after.as_deref()).await
}

// ===========================================================================
// One row per group
// ===========================================================================

/// Maintain an aggregated view: one row per group, whose stored stats this
/// write adjusts by the delta the record made.
///
/// The record's group is derived from each image separately, because a record
/// can move between groups, enter the view or leave it, and each of those is a
/// different pair of adjustments. A group the record does not belong to under
/// an image is `None`, which is what the four cases below are over.
async fn aggregated(
	scope: &Arc<WriteScope>,
	ctx: &WriteCtx,
	frame: &ExecutionContext,
	target: &Aggregated<'_>,
	images: &Images,
	action: Action,
) -> Result<(), ControlFlow> {
	match action {
		Action::Create => {
			let Some(group) = group_of(target, frame, &images.current).await? else {
				return Ok(());
			};
			group_create(scope, ctx, frame, target, images, group).await
		}
		Action::Delete => {
			let Some(group) = group_of(target, frame, &images.initial).await? else {
				return Ok(());
			};
			group_delete(scope, ctx, frame, target, images, group).await
		}
		Action::Update => {
			let before = group_of(target, frame, &images.initial).await?;
			let after = group_of(target, frame, &images.current).await?;
			match (before, after) {
				// The record was outside the view before and stays outside.
				(None, None) => Ok(()),
				(Some(before), Some(after)) if before == after => {
					group_update(scope, ctx, frame, target, images, before).await
				}
				// The record moved between groups: it leaves one and joins the
				// other, which is a delete and a create rather than an update.
				(Some(before), Some(after)) => {
					group_delete(scope, ctx, frame, target, images, before).await?;
					group_create(scope, ctx, frame, target, images, after).await
				}
				(Some(before), None) => {
					group_delete(scope, ctx, frame, target, images, before).await
				}
				(None, Some(after)) => group_create(scope, ctx, frame, target, images, after).await,
			}
		}
	}
}

/// The group one image belongs to, or `None` when the view's condition
/// excludes it.
async fn group_of(
	target: &Aggregated<'_>,
	frame: &ExecutionContext,
	image: &Value,
) -> Result<Option<Vec<Value>>, ControlFlow> {
	if let Some(condition) = &target.view.condition
		&& !evaluate(condition, frame, image).await?.is_truthy()
	{
		return Ok(None);
	}
	let mut group = Vec::with_capacity(target.view.group.len());
	for expr in &target.view.group {
		group.push(evaluate(expr, frame, image).await?);
	}
	Ok(Some(group))
}

/// A record joined the group: add its arguments to the stored stats.
///
/// A group no row was in yet starts from the analysis's own stats, which is
/// how a view table gains its first row for a group.
async fn group_create(
	scope: &Arc<WriteScope>,
	ctx: &WriteCtx,
	frame: &ExecutionContext,
	target: &Aggregated<'_>,
	images: &Images,
	group: Vec<Value>,
) -> Result<(), ControlFlow> {
	let key = RecordIdKey::Array(Array(group.clone()));
	let (mut record, action) = match read_view_row(ctx, target.sources.table, &key).await? {
		Some(record) => ((*record).clone(), Action::Update),
		None => (new_group(target.sources.analysis), Action::Create),
	};
	let before = record.clone();
	let stats = stats_of(&mut record)?;
	let arguments = arguments_of(target, frame, &images.current).await?;
	add_to_aggregation_stats(&arguments, stats).map_err(ControlFlow::Err)?;
	store_group(scope, ctx, frame, target, key, record, before, action).await
}

/// A record left the group: take its arguments back out of the stored stats,
/// and drop the row entirely when it was the group's last record.
///
/// A stat the removal cannot maintain from the change alone — the value
/// leaving *was* the extreme — is recomputed over the group's remaining rows.
async fn group_delete(
	scope: &Arc<WriteScope>,
	ctx: &WriteCtx,
	frame: &ExecutionContext,
	target: &Aggregated<'_>,
	images: &Images,
	group: Vec<Value>,
) -> Result<(), ControlFlow> {
	let table = target.sources.table;
	let key = RecordIdKey::Array(Array(group.clone()));
	let Some(stored) = read_view_row(ctx, table, &key).await? else {
		return Err(left_no_row(&group, table));
	};
	let mut record = (*stored).clone();
	let before = record.clone();
	let stats = stats_of(&mut record)?;
	let Some(count) = AggregationStat::get_count(stats) else {
		return Err(no_record_count(table));
	};
	if count == 1 {
		// The last record of the group: the row goes rather than being
		// rewritten with empty stats.
		let ns = ctx.ns.namespace_id;
		let db = ctx.db.database_id;
		ctx.txn().del_record(ns, db, table, &key).await.map_err(ControlFlow::Err)?;
		let row = RecordId {
			table: table.clone(),
			key,
		};
		return scope.run_view_row(table, row, Action::Delete, Some(&before), None).await;
	}

	let arguments = arguments_of(target, frame, &images.initial).await?;
	let recalculations =
		remove_from_aggregation_stats(&arguments, stats).map_err(ControlFlow::Err)?;
	recalculate(scope, frame, &target.sources, &group, stats, &recalculations).await?;
	store_group(scope, ctx, frame, target, key, record, before, Action::Update).await
}

/// A record changed within its group: apply the delta between the arguments it
/// contributed before and the ones it contributes now.
async fn group_update(
	scope: &Arc<WriteScope>,
	ctx: &WriteCtx,
	frame: &ExecutionContext,
	target: &Aggregated<'_>,
	images: &Images,
	group: Vec<Value>,
) -> Result<(), ControlFlow> {
	let key = RecordIdKey::Array(Array(group.clone()));
	let Some(stored) = read_view_row(ctx, target.sources.table, &key).await? else {
		return Err(changed_no_row(&group, target.sources.table));
	};
	let mut record = (*stored).clone();
	let before = record.clone();
	let stats = stats_of(&mut record)?;
	let was = arguments_of(target, frame, &images.initial).await?;
	let now = arguments_of(target, frame, &images.current).await?;
	let recalculations = update_aggregation_stats(&was, &now, stats).map_err(ControlFlow::Err)?;
	recalculate(scope, frame, &target.sources, &group, stats, &recalculations).await?;
	store_group(scope, ctx, frame, target, key, record, before, Action::Update).await
}

/// Render a group's stats into the view row and store it, then run the view
/// table's own lifecycle over it.
#[allow(clippy::too_many_arguments)]
async fn store_group(
	scope: &Arc<WriteScope>,
	ctx: &WriteCtx,
	frame: &ExecutionContext,
	target: &Aggregated<'_>,
	key: RecordIdKey,
	mut record: Record,
	before: Record,
	action: Action,
) -> Result<(), ControlFlow> {
	let stats = stats_of(&mut record)?;
	// The field list is evaluated against the document the group's stats make
	// — the group key under its own names and one value per stat — rather than
	// against any record of the group.
	let document = Value::Object(aggregation::create_field_document(&group_values(&key), stats));
	let mut data = Value::empty_object();
	for (place, expr) in &target.view.fields {
		let value = evaluate(expr, frame, &document).await?;
		apply::set(&mut data, place.steps(), value, frame).await?;
	}
	record.data = data;

	let ns = ctx.ns.namespace_id;
	let db = ctx.db.database_id;
	let table = target.sources.table;
	let record = Arc::new(record);
	ctx.txn()
		.set_record(ns, db, table, &key, Arc::clone(&record))
		.await
		.map_err(ControlFlow::Err)?;

	let row = RecordId {
		table: table.clone(),
		key,
	};
	let before = match action {
		Action::Create => None,
		Action::Update | Action::Delete => Some(&before),
	};
	scope.run_view_row(table, row, action, before, Some(&record)).await
}

/// Recompute the extremes the stored stats can no longer maintain from the
/// change alone, and write them back.
///
/// Nothing to do when the change kept every stat maintainable, which is the
/// common case: only a value that *was* the group's extreme forces a re-read.
pub(crate) async fn recalculate(
	scope: &Arc<WriteScope>,
	frame: &ExecutionContext,
	target: &ViewSources<'_>,
	group: &[Value],
	stats: &mut [AggregationStat],
	recalculations: &[Recalculation],
) -> Result<(), ControlFlow> {
	if recalculations.is_empty() {
		return Ok(());
	}
	let values = recalculate_aggregates(scope, frame, target, group, recalculations).await?;
	apply_recalculations(stats, recalculations, values).map_err(ControlFlow::Err)
}

/// Read the group's remaining rows and recompute one value per entry of
/// `recalculations`.
///
/// The rows read are the group's as the view defines it: every table the view
/// selects from, narrowed by an equality per group key and by the view's own
/// condition. An extreme is only meaningful over exactly the rows the view
/// holds, so the statement has to name the same rows the view's own query
/// would.
///
/// `GROUP ALL` answers with one row whatever reaches it, so an extreme
/// reported over no rows is the aggregate's answer for no values rather than
/// the group's. A count accompanies the extremes to tell the two apart, and a
/// group the statement finds empty while the stored count says it still has
/// rows is a disagreement this path cannot resolve: the write fails rather
/// than recording a bound no row holds.
///
/// The statement is compiled once per scope for each set of extremes it
/// recomputes, the group's key values bound as `$__view_group_<n>`.
async fn recalculate_aggregates(
	scope: &Arc<WriteScope>,
	frame: &ExecutionContext,
	target: &ViewSources<'_>,
	group: &[Value],
	recalculations: &[Recalculation],
) -> Result<Vec<Value>, ControlFlow> {
	let analysis = target.analysis;
	let names: Vec<String> = (0..analysis.group_expressions.len())
		.map(|index| format!("__view_group_{index}"))
		.collect();
	let key = NestedKey::ViewRecalculation {
		view: target.table.clone(),
		extremes: recalculations
			.iter()
			.map(|recalculation| (recalculation.function.clone(), recalculation.arg))
			.collect(),
	};
	let frame = frame.with_params(names.iter().cloned().zip(group.iter().cloned()));
	let bound: Vec<&str> = names.iter().map(String::as_str).collect();
	let plan = scope
		.nested_plan(key, &frame, &bound, || recalculation_of(target, recalculations, &names))
		.await?;
	let mut rows = run_nested(&plan, &frame).await?;

	// `GROUP ALL` collapses whatever reaches it into exactly one row.
	let invalid = || {
		ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(
			"recalculating an aggregate returned an invalid result".to_string()
		)))
	};
	if rows.len() != 1 {
		return Err(invalid());
	}
	let Some(Value::Array(Array(mut values))) = rows.pop() else {
		return Err(invalid());
	};
	if values.len() != recalculations.len() + 1 {
		return Err(invalid());
	}
	let Some(Value::Number(found)) = values.pop() else {
		return Err(invalid());
	};
	if found.as_int() == 0 {
		return Err(ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(
			"recalculating an aggregate found no row for the group it recalculates".to_string()
		))));
	}
	Ok(values)
}

/// The statement [`recalculate_aggregates`] runs: `SELECT VALUE [extremes…,
/// count()]` over the view's sources, narrowed to the group whose key values
/// are the parameters `names` and by the view's own condition, `GROUP ALL`.
pub(crate) fn recalculation_of(
	target: &ViewSources<'_>,
	recalculations: &[Recalculation],
	names: &[String],
) -> Result<LogicalOp, ControlFlow> {
	let analysis = target.analysis;
	let mut exprs: Vec<Expr> = recalculations
		.iter()
		.map(|recalculation| {
			Expr::FunctionCall(Box::new(FunctionCall {
				receiver: Function::Normal(recalculation.function.clone()),
				arguments: vec![analysis.aggregate_arguments[recalculation.arg].clone()],
			}))
		})
		.collect();
	exprs.push(Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal("count".to_string()),
		arguments: Vec::new(),
	})));

	// The group-key equalities lead and the view's condition is ANDed last:
	// `&&` short-circuits left to right, so a row of another group is rejected
	// on a key comparison without the view's condition being evaluated for it.
	let mut cond: Option<Expr> = None;
	for (index, expr) in analysis.group_expressions.iter().enumerate() {
		let equality = Expr::Binary {
			left: Box::new(expr.clone()),
			op: BinaryOperator::Equal,
			right: Box::new(Expr::Param(Param::from(names[index].clone()))),
		};
		cond = Some(conjoin(cond, equality));
	}
	if let Some(view_condition) = target.condition.cloned() {
		cond = Some(conjoin(cond, view_condition));
	}

	let statement = SelectStatement {
		// SELECT VALUE [recalc1, .., count()]
		fields: Fields::Value(Box::new(Selector {
			expr: Expr::Literal(Literal::Array(exprs)),
			alias: None,
		})),
		// FROM source1, source2, .. — `ONLY` is rejected for several targets
		// without a limit, so the single `GROUP ALL` row is taken below.
		only: false,
		what: target.tables.iter().cloned().map(Expr::Table).collect(),
		cond: cond.map(Cond),
		group: Some(Groups(Vec::new())),
		omit: vec![],
		with: None,
		split: None,
		order: None,
		limit: None,
		start: None,
		fetch: None,
		version: Expr::Literal(Literal::None),
		timeout: Expr::Literal(Literal::None),
		explain: None,
		tempfiles: false,
		for_update: false,
	};
	crate::expr::logical::lower_select(statement.into())
		.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))
}

/// `left && right`, or `right` alone when there is no left.
fn conjoin(left: Option<Expr>, right: Expr) -> Expr {
	match left {
		Some(left) => Expr::Binary {
			left: Box::new(left),
			op: BinaryOperator::And,
			right: Box::new(right),
		},
		None => right,
	}
}

// ===========================================================================
// Shared
// ===========================================================================

/// Evaluate one view clause against `subject`, which it reads as its document.
async fn evaluate(
	expr: &Arc<dyn PhysicalExpr>,
	frame: &ExecutionContext,
	subject: &Value,
) -> Result<Value, ControlFlow> {
	expr.evaluate(EvalContext::from_exec_ctx(frame).with_value_and_doc(subject))
		.await
		.catch_return()
		.map_err(ControlFlow::Err)
}

/// The arguments this record contributes to the group's stats, under one
/// image.
async fn arguments_of(
	target: &Aggregated<'_>,
	frame: &ExecutionContext,
	image: &Value,
) -> Result<Vec<Value>, ControlFlow> {
	let mut arguments = Vec::with_capacity(target.view.arguments.len());
	for expr in &target.view.arguments {
		arguments.push(evaluate(expr, frame, image).await?);
	}
	Ok(arguments)
}

/// A view row as it is stored.
///
/// Decoded raw rather than through the record decoder, because a view row's
/// `data` carries no `id`: the row's identity is its key, and splicing it back
/// in would change what the view table's own events and subscribers are shown.
pub(crate) async fn read_view_row(
	ctx: &WriteCtx,
	table: &TableName,
	key: &RecordIdKey,
) -> Result<Option<Arc<Record>>, ControlFlow> {
	let record_key = RecordKey {
		ns: ctx.ns.namespace_id,
		db: ctx.db.database_id,
		tb: Cow::Borrowed(table),
		id: Cow::Borrowed(key),
	};
	match ctx.txn().get_key_raw(&record_key, None).await.map_err(ControlFlow::Err)? {
		Some(bytes) => Ok(Some(Arc::new(
			revision::from_slice::<Record>(&bytes)
				.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))?,
		))),
		None => Ok(None),
	}
}

/// The row a group no record was in yet starts from: no data, and the
/// analysis's own stats.
pub(crate) fn new_group(analysis: &AggregationAnalysis) -> Record {
	Record {
		data: Value::None,
		metadata: Some(Metadata {
			record_type: RecordType::Table,
			aggregation_stats: analysis
				.aggregations
				.iter()
				.map(|aggregation| aggregation.to_stat())
				.collect(),
		}),
	}
}

/// The failure of a record leaving `group` of `table`, which holds no row for
/// the group.
pub(crate) fn left_no_row(group: &[Value], table: &TableName) -> ControlFlow {
	ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(format!(
		"a record left the group {} of the view {}, which holds no row for it",
		Value::Array(Array(group.to_vec())).to_sql(),
		table
	))))
}

/// The failure of a record changing within `group` of `table`, which holds no
/// row for the group.
pub(crate) fn changed_no_row(group: &[Value], table: &TableName) -> ControlFlow {
	ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(format!(
		"a record changed within the group {} of the view {}, which holds no row for it",
		Value::Array(Array(group.to_vec())).to_sql(),
		table
	))))
}

/// The failure of a row of `table` whose stats hold no record count.
pub(crate) fn no_record_count(table: &TableName) -> ControlFlow {
	ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(format!(
		"the view {table} holds a row with no record count"
	))))
}

/// The stored stats of a view row, which every aggregated row carries.
pub(crate) fn stats_of(record: &mut Record) -> Result<&mut [AggregationStat], ControlFlow> {
	match record.metadata.as_mut() {
		Some(metadata) => Ok(&mut metadata.aggregation_stats),
		None => Err(ControlFlow::Err(anyhow::anyhow!(crate::err::EngineError::unreachable(
			"a row of an aggregated view carries no stats"
		)))),
	}
}

/// The group values a view row's key is made of.
///
/// An aggregated view keys each group row by the array of its group values, so
/// a key of any other shape belongs to no group and names none.
#[allow(
	clippy::wildcard_enum_match_arm,
	reason = "only an array key carries group values; every other key shape carries none"
)]
pub(crate) fn group_values(key: &RecordIdKey) -> Vec<Value> {
	match key {
		RecordIdKey::Array(Array(values)) => values.clone(),
		_ => Vec::new(),
	}
}
