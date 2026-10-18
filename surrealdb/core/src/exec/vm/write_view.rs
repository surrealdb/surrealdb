//! The host's side of a table's views maintained as instructions.
//!
//! A write keeps every view its table feeds in step with the record, as the
//! views stage does: a materialized view holds one row per source record under
//! the record's own key, and an aggregated view one row per group, whose stats
//! the write adjusts by the change the record made. The views are the ones the
//! table held when the program was lowered, and each view's clauses — its
//! condition, projection, group key, aggregate arguments and fields — are host
//! pipelines of the program, run in the view frame (permissions off, writes
//! allowed, the writer's authority) over the image each is about.
//!
//! A row the maintenance writes or removes is then a write of its own on the
//! view table: [`WriteForm::View`], whose subject is the row the view computed
//! and whose action the maintenance settled. The view table's order — its
//! indexes, its own views, events, live queries and change feed — is a unit
//! lowered from the view table's definitions with the writing program
//! ([`ViewRows`]), one pipeline per action, and each row runs through it over a
//! write state of its own, stacked on the writer's, in the view table's
//! view-maintenance write context.
//!
//! An extreme a removed record held is recomputed over the group's remaining
//! rows by the aggregated view's recalculation unit ([`Recalc`]), lowered with
//! the program, which reads the group's key values from the frame it runs in.
//!
//! Each view row's write carries the view tables whose rows are in flight
//! beneath it. A view among them reads itself, and the write that reaches it
//! again fails ([`refuse_reentry`]); every level spends of the computation-depth
//! budget what a nested write does, so a chain of views deeper than the budget
//! fails too ([`view_row_exec`]). Either is refused where it is reached, since
//! every level holds its frames on the stack while the levels within it run.
//!
//! A table whose views changed since the program was lowered fails the record:
//! the program's clauses are not the new views'.
//!
//! [`WriteForm::View`]: crate::exec::operators::write::plan::WriteForm::View

// Runtime the IR lowering emits.
#![allow(dead_code)]

use std::sync::Arc;

use super::data::PlaceRun;
use super::storage::{del_view_row, set_view_row};
use super::{State, WriteState, internal, run_image_in_frame};
use crate::catalog::aggregation::{
	self, AggregationAnalysis, AggregationStat, Recalculation, add_to_aggregation_stats,
	apply_recalculations, remove_from_aggregation_stats, update_aggregation_stats,
};
use crate::catalog::{Record, TableDefinition, ViewDefinition};
use crate::doc::Action;
use crate::exec::operators::write::apply;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::row::WriteRow;
use crate::exec::operators::write::scope::{WritePurpose, WriteScope};
use crate::exec::operators::write::stages::observe::is_observed;
use crate::exec::operators::write::stages::view::{
	changed_no_row, group_values, left_no_row, new_group, no_record_count, read_view_row, stats_of,
};
use crate::exec::permission::view_frame;
use crate::exec::vm::depth::STATEMENT_DEPTH;
use crate::exec::vm::host::{admitted_unit, gather_program, unit_table};
use crate::exec::vm::program::{PlaceStepSpec, Program, UnitId};
use crate::exec::{Error as ExecError, ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::val::{Array, RecordId, RecordIdKey, TableName, Value};

/// A table's views, as the program was lowered from them.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) struct LoweredViews {
	/// The table's view definitions when the program was lowered, in the
	/// order the catalog holds them.
	pub(in crate::exec::vm) definitions: Arc<[TableDefinition]>,
	/// Each view's maintenance, one per definition.
	pub(in crate::exec::vm) views: Vec<LoweredView>,
}

/// One view's maintenance: its clauses as host pipelines of the program, and
/// the view table's own write order.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) enum LoweredView {
	/// A view nothing maintains.
	Unmaintained,
	/// One row per source record.
	Materialized {
		/// The view's condition over the record's post-image, `None` for a
		/// view that keeps every record the write did not delete.
		condition: Option<ViewClause>,
		/// The row the view holds for the record.
		projection: ViewClause,
		rows: ViewRows,
	},
	/// One row per group.
	Aggregated {
		/// The view's condition over an image, `None` for a view every record
		/// belongs to.
		condition: Option<ViewClause>,
		/// Each `GROUP BY` target over an image, in order.
		group: Vec<ViewClause>,
		/// Each argument the stored aggregates accumulate, over an image.
		arguments: Vec<ViewClause>,
		/// Each field over the document a group's stats make, and the place
		/// it is written to.
		fields: Vec<(Vec<PlaceStepSpec>, ViewClause)>,
		/// The recomputation of the extremes the view's stats keep, `None`
		/// for a view that keeps none.
		recalc: Option<Recalc>,
		rows: ViewRows,
	},
}

assert_plain!(LoweredViews);

/// One of a view's clauses as host pipelines over the image it is about: one
/// for an image that is an object, whose fields the pipeline reads itself, and
/// one for any value — the image of a record that is not there is `NONE`. A
/// clause evaluated in its compiled form is one pipeline for both.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::exec::vm) struct ViewClause {
	pub(in crate::exec::vm) object: u16,
	pub(in crate::exec::vm) any: u16,
}

/// A view table's own write order, lowered from the view table's definitions:
/// a unit of the writing program whose pipelines are the order for a created,
/// an updated and a deleted row, in that order, each answering whether it kept
/// the row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::exec::vm) struct ViewRows {
	pub(in crate::exec::vm) unit: UnitId,
}

assert_plain!(ViewRows);

/// The recomputation of the extremes an aggregated view's stats keep: a unit
/// of the writing program whose statement reads, over the rows the view holds
/// a group of, the value of each of `extremes` and then the group's row
/// count, with the group's key values bound to the parameters
/// `__view_group_<n>`.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) struct Recalc {
	pub(in crate::exec::vm) unit: UnitId,
	/// The aggregate function and aggregate argument of each extreme the unit
	/// reads, in the order it reads them.
	pub(in crate::exec::vm) extremes: Vec<(String, usize)>,
}

assert_plain!(Recalc);

/// The pipeline of [`ViewRows`] that takes a row written with
/// `action`.
pub(in crate::exec::vm) fn action_pipeline(action: Action) -> usize {
	match action {
		Action::Create => 0,
		Action::Update => 1,
		Action::Delete => 2,
	}
}

/// What every view of one record's maintenance reads: the program whose host
/// pipelines the clauses are, the writing statement's scope, the source
/// table's write context, the view frame, and the view tables whose rows are
/// in flight beneath the record's write ([`WriteState::view_row`]).
struct Maintain<'a> {
	program: &'a Program,
	scope: Arc<WriteScope>,
	ctx: Arc<WriteCtx>,
	frame: ExecutionContext,
	maintaining: Arc<[TableName]>,
}

/// Maintain every view the record in flight's table feeds, for `action`.
///
/// Nothing runs for a write no observer sees, as for a table with no views.
pub(super) async fn views(
	program: &Program,
	action: Action,
	lowered: &Arc<LoweredViews>,
	state: &mut State,
) -> FlowResult<()> {
	let write =
		state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	let (row, ctx) = write.in_flight()?;
	if ctx.views.is_empty() || !is_observed(row, &ctx) {
		return Ok(());
	}
	if !write.views_hold(&ctx.views, lowered) {
		return Err(internal("the views of the table changed after the write was lowered"));
	}
	let maintaining = Arc::clone(&write.maintaining);
	let (row, ctx) = write.in_flight()?;
	let Some(id) = row.id.clone() else {
		return Ok(());
	};
	let initial = row.initial.clone();
	let current = row.current.clone();
	let m = Maintain {
		program,
		scope: ctx.scope()?,
		frame: view_frame(ctx.exec()),
		ctx,
		maintaining,
	};
	// Every clause runs in the view frame, which the state holds while the
	// views are maintained.
	let outer = state.frame.replace(m.frame.clone());
	let maintained = maintain(&m, lowered, state, &id, &initial, &current, action).await;
	state.frame = outer;
	maintained
}

/// Maintain each of `lowered`'s views for the record `id` whose images are
/// `initial` and `current`. A view whose row is already in flight beneath the
/// record's write fails it before anything of it is evaluated
/// ([`refuse_reentry`]).
async fn maintain(
	m: &Maintain<'_>,
	lowered: &LoweredViews,
	state: &mut State,
	id: &RecordId,
	initial: &Value,
	current: &Value,
	action: Action,
) -> FlowResult<()> {
	for (definition, view) in lowered.definitions.iter().zip(&lowered.views) {
		if !matches!(view, LoweredView::Unmaintained) {
			refuse_reentry(&m.maintaining, &definition.name)?;
		}
		match view {
			LoweredView::Unmaintained => {}
			LoweredView::Materialized {
				condition,
				projection,
				rows,
			} => {
				let row = Materialized {
					table: &definition.name,
					condition: *condition,
					projection: *projection,
					rows,
				};
				materialized(m, state, &row, id, current, action).await?;
			}
			LoweredView::Aggregated {
				condition,
				group,
				arguments,
				fields,
				recalc,
				rows,
			} => {
				let Some(ViewDefinition::Aggregated {
					analysis,
					..
				}) = &definition.view
				else {
					return Err(internal("an aggregated view whose definition is not one"));
				};
				let target = Aggregated {
					analysis,
					table: &definition.name,
					condition: *condition,
					group,
					arguments,
					fields,
					recalc: recalc.as_ref(),
					rows,
				};
				aggregated(m, state, &target, initial, current, action).await?;
			}
		}
	}
	Ok(())
}

/// One materialized view's maintenance.
struct Materialized<'a> {
	table: &'a TableName,
	condition: Option<ViewClause>,
	projection: ViewClause,
	rows: &'a ViewRows,
}

/// Maintain a materialized view: the row under the source record's key is
/// written when the view's condition admits the record's post-image — every
/// record the write did not delete, for a view with none — and removed
/// otherwise. The view row's own action is what the view table held: a record
/// the condition newly admits creates its row whatever the statement did.
async fn materialized(
	m: &Maintain<'_>,
	state: &mut State,
	view: &Materialized<'_>,
	id: &RecordId,
	current: &Value,
	action: Action,
) -> FlowResult<()> {
	let key = id.key.clone();
	let admitted = match view.condition {
		Some(condition) => clause(m, condition, current, state).await?.is_truthy(),
		None => action != Action::Delete,
	};
	// Read before it is overwritten: it is the view row's before-image, and
	// whether it was there decides whether the write creates or updates it.
	let before = read_view_row(&m.ctx, view.table, &key).await?;
	let (view_action, after) = if admitted {
		let data = clause(m, view.projection, current, state).await?;
		let record = Record::new(data);
		set_view_row(&m.ctx, view.table, &key, &record, before.is_none()).await?;
		let action = match before {
			Some(_) => Action::Update,
			None => Action::Create,
		};
		(action, Some(record))
	} else {
		del_view_row(&m.ctx, view.table, &key).await?;
		// Removing a row the view never held changes nothing.
		if before.is_none() {
			return Ok(());
		}
		(Action::Delete, None)
	};
	let row = RecordId {
		table: view.table.clone(),
		key,
	};
	run_view_row(m, state, view.rows, row, view_action, before.as_deref(), after.as_ref()).await
}

/// One aggregated view's maintenance.
struct Aggregated<'a> {
	/// The view's stored analysis, which says how many stats a new group
	/// starts with.
	analysis: &'a AggregationAnalysis,
	/// The view table.
	table: &'a TableName,
	condition: Option<ViewClause>,
	group: &'a [ViewClause],
	arguments: &'a [ViewClause],
	fields: &'a [(Vec<PlaceStepSpec>, ViewClause)],
	recalc: Option<&'a Recalc>,
	rows: &'a ViewRows,
}

/// Maintain an aggregated view: the record's group under each image, and the
/// stats of the groups it joined, left or changed within adjusted by what it
/// contributes. A record that moved between groups leaves one and joins the
/// other.
async fn aggregated(
	m: &Maintain<'_>,
	state: &mut State,
	view: &Aggregated<'_>,
	initial: &Value,
	current: &Value,
	action: Action,
) -> FlowResult<()> {
	match action {
		Action::Create => {
			let Some(group) = group_of(m, view, current, state).await? else {
				return Ok(());
			};
			group_create(m, state, view, current, group).await
		}
		Action::Delete => {
			let Some(group) = group_of(m, view, initial, state).await? else {
				return Ok(());
			};
			group_delete(m, state, view, initial, group).await
		}
		Action::Update => {
			let before = group_of(m, view, initial, state).await?;
			let after = group_of(m, view, current, state).await?;
			match (before, after) {
				(None, None) => Ok(()),
				(Some(before), Some(after)) if before == after => {
					group_update(m, state, view, initial, current, before).await
				}
				(Some(before), Some(after)) => {
					group_delete(m, state, view, initial, before).await?;
					group_create(m, state, view, current, after).await
				}
				(Some(before), None) => group_delete(m, state, view, initial, before).await,
				(None, Some(after)) => group_create(m, state, view, current, after).await,
			}
		}
	}
}

/// The group `image` belongs to, or `None` when the view's condition excludes
/// it.
async fn group_of(
	m: &Maintain<'_>,
	view: &Aggregated<'_>,
	image: &Value,
	state: &mut State,
) -> FlowResult<Option<Vec<Value>>> {
	if let Some(condition) = view.condition
		&& !clause(m, condition, image, state).await?.is_truthy()
	{
		return Ok(None);
	}
	let mut group = Vec::with_capacity(view.group.len());
	for pipeline in view.group {
		group.push(clause(m, *pipeline, image, state).await?);
	}
	Ok(Some(group))
}

/// The arguments the record contributes to its group's stats under `image`.
async fn arguments_of(
	m: &Maintain<'_>,
	view: &Aggregated<'_>,
	image: &Value,
	state: &mut State,
) -> FlowResult<Vec<Value>> {
	let mut arguments = Vec::with_capacity(view.arguments.len());
	for pipeline in view.arguments {
		arguments.push(clause(m, *pipeline, image, state).await?);
	}
	Ok(arguments)
}

/// A record joined `group`: its arguments added to the group's stats, from
/// the analysis's own for a group no row was in yet.
async fn group_create(
	m: &Maintain<'_>,
	state: &mut State,
	view: &Aggregated<'_>,
	current: &Value,
	group: Vec<Value>,
) -> FlowResult<()> {
	let key = RecordIdKey::Array(Array(group));
	let (mut record, action) = match read_view_row(&m.ctx, view.table, &key).await? {
		Some(record) => ((*record).clone(), Action::Update),
		None => (new_group(view.analysis), Action::Create),
	};
	let before = record.clone();
	let stats = stats_of(&mut record)?;
	let arguments = arguments_of(m, view, current, state).await?;
	add_to_aggregation_stats(&arguments, stats).map_err(ControlFlow::Err)?;
	store_group(m, state, view, key, record, before, action).await
}

/// A record left `group`: its arguments taken back out of the group's stats,
/// an extreme it held recomputed over the group's other records, and the row
/// removed when it was the group's last record.
async fn group_delete(
	m: &Maintain<'_>,
	state: &mut State,
	view: &Aggregated<'_>,
	initial: &Value,
	group: Vec<Value>,
) -> FlowResult<()> {
	let table = view.table;
	let key = RecordIdKey::Array(Array(group.clone()));
	let Some(stored) = read_view_row(&m.ctx, table, &key).await? else {
		return Err(left_no_row(&group, table));
	};
	let mut record = (*stored).clone();
	let before = record.clone();
	let stats = stats_of(&mut record)?;
	let Some(count) = AggregationStat::get_count(stats) else {
		return Err(no_record_count(table));
	};
	if count == 1 {
		del_view_row(&m.ctx, table, &key).await?;
		let row = RecordId {
			table: table.clone(),
			key,
		};
		return run_view_row(m, state, view.rows, row, Action::Delete, Some(&before), None).await;
	}
	let arguments = arguments_of(m, view, initial, state).await?;
	let recalculations =
		remove_from_aggregation_stats(&arguments, stats).map_err(ControlFlow::Err)?;
	recalculate(m, state, view, &group, stats, &recalculations).await?;
	store_group(m, state, view, key, record, before, Action::Update).await
}

/// A record changed within `group`: the change between what it contributed
/// and what it contributes under the current image applied to the group's stats.
async fn group_update(
	m: &Maintain<'_>,
	state: &mut State,
	view: &Aggregated<'_>,
	initial: &Value,
	current: &Value,
	group: Vec<Value>,
) -> FlowResult<()> {
	let key = RecordIdKey::Array(Array(group.clone()));
	let Some(stored) = read_view_row(&m.ctx, view.table, &key).await? else {
		return Err(changed_no_row(&group, view.table));
	};
	let mut record = (*stored).clone();
	let before = record.clone();
	let stats = stats_of(&mut record)?;
	let was = arguments_of(m, view, initial, state).await?;
	let now = arguments_of(m, view, current, state).await?;
	let recalculations = update_aggregation_stats(&was, &now, stats).map_err(ControlFlow::Err)?;
	recalculate(m, state, view, &group, stats, &recalculations).await?;
	store_group(m, state, view, key, record, before, Action::Update).await
}

/// Render a group's stats into its row — each field evaluated against the
/// document the group key and stats make, and written to its place — store
/// the row, and run it through the view table's order.
async fn store_group(
	m: &Maintain<'_>,
	state: &mut State,
	view: &Aggregated<'_>,
	key: RecordIdKey,
	mut record: Record,
	before: Record,
	action: Action,
) -> FlowResult<()> {
	let document = Value::Object(aggregation::create_field_document(
		&group_values(&key),
		stats_of(&mut record)?,
	));
	let mut data = Value::empty_object();
	for (steps, pipeline) in view.fields {
		let value = clause(m, *pipeline, &document, state).await?;
		let mut run = PlaceRun {
			program: m.program,
			state,
			exec: &m.frame,
		};
		apply::set_with(&mut data, steps, value, &mut run).await?;
	}
	record.data = data;
	set_view_row(&m.ctx, view.table, &key, &record, action == Action::Create).await?;
	let row = RecordId {
		table: view.table.clone(),
		key,
	};
	let before = match action {
		Action::Create => None,
		Action::Update | Action::Delete => Some(&before),
	};
	run_view_row(m, state, view.rows, row, action, before, Some(&record)).await
}

/// Recompute the extremes of `recalculations`, which the stats of `group` of
/// the aggregated view `view` cannot maintain from the change alone, over
/// the group's remaining rows by the view's recalculation unit, and write them
/// back into `stats`.
///
/// The unit reads the rows the view holds the group of: every table the view
/// selects from, narrowed by an equality per group key and by the view's own
/// condition. `GROUP ALL` answers with one row whatever reaches it, so an
/// extreme reported over no rows is the aggregate's answer for no values rather
/// than the group's; a count accompanies the extremes to tell the two apart,
/// and a group the unit finds empty while the stored count says it still has
/// rows is a disagreement this path cannot resolve: the write fails rather than
/// recording a bound no row holds.
async fn recalculate(
	m: &Maintain<'_>,
	state: &mut State,
	view: &Aggregated<'_>,
	group: &[Value],
	stats: &mut [AggregationStat],
	recalculations: &[Recalculation],
) -> FlowResult<()> {
	if recalculations.is_empty() {
		return Ok(());
	}
	let recalc = view
		.recalc
		.ok_or_else(|| internal("a recalculation of a view whose stats keep no extreme"))?;
	let invalid = || {
		ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatement(
			"recalculating an aggregate returned an invalid result".to_string()
		)))
	};
	let names = (0..group.len()).map(|index| format!("__view_group_{index}"));
	let frame = m.frame.with_params(names.zip(group.iter().cloned()));
	let units = unit_table(m.program, state, &frame)?;
	let program = Arc::clone(admitted_unit(&units, recalc.unit, &frame)?);
	// A unit that holds no arena of its own finds the units its ids name in the
	// table it was found in.
	let frame = if program.units.is_empty() {
		frame.with_vm_units(Arc::clone(&units))
	} else {
		frame
	};
	let gathered = crate::exec::trampoline::nest(gather_program(program, frame)).await?;
	let Value::Array(Array(mut rows)) = gathered else {
		return Err(invalid());
	};
	if rows.len() != 1 {
		return Err(invalid());
	}
	let Some(Value::Array(Array(mut values))) = rows.pop() else {
		return Err(invalid());
	};
	if values.len() != recalc.extremes.len() + 1 {
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
	let mut wanted = Vec::with_capacity(recalculations.len());
	for recalculation in recalculations {
		let at = recalc
			.extremes
			.iter()
			.position(|(function, arg)| {
				*function == recalculation.function && *arg == recalculation.arg
			})
			.ok_or_else(|| internal("an extreme the view's recalculation does not read"))?;
		wanted.push(values[at].clone());
	}
	apply_recalculations(stats, recalculations, wanted).map_err(ControlFlow::Err)
}

/// Evaluate one of a view's clauses, `clause`, against `subject` in the view
/// frame, which the state holds. A `RETURN` inside it is its value; a `BREAK`
/// or `CONTINUE` has no loop to leave.
async fn clause(
	m: &Maintain<'_>,
	clause: ViewClause,
	subject: &Value,
	state: &mut State,
) -> FlowResult<Value> {
	let pipeline = match subject {
		Value::Object(_) => clause.object,
		_ => clause.any,
	};
	let exec = m.ctx.exec();
	match run_image_in_frame(m.program, pipeline, subject.clone(), state, exec).await {
		Ok(value) | Err(ControlFlow::Return(value)) => Ok(value),
		Err(ControlFlow::Break | ControlFlow::Continue) => {
			Err(ControlFlow::Err(anyhow::Error::new(crate::exec::Error::InvalidControlFlow)))
		}
		Err(failure) => Err(failure),
	}
}

/// Fail with [`ExecError::ViewReadsItself`] where `view` is among
/// `maintaining`, the view tables whose rows are in flight beneath the write
/// that would maintain it, each read by the next and the last the write's own
/// table: every row maintaining `view` writes would reach it again, without
/// end.
///
/// Each view row is written within the write that fed it, holding its frames
/// on the stack while the views over it are maintained, so the re-entry is
/// refused where it is reached rather than left to exhaust the stack. The
/// refusal is exact: views reached twice down different paths, as two views
/// over one source and a third over both, are not in flight at once.
pub(super) fn refuse_reentry(maintaining: &[TableName], view: &TableName) -> FlowResult<()> {
	let Some(at) = maintaining.iter().position(|table| table == view) else {
		return Ok(());
	};
	// `maintaining[at + 1..]` each read the one before: `view` reads the last of
	// them, which reads the one before it, back to the one that reads `view`.
	let through = maintaining[at + 1..].iter().rev().map(|table| table.as_str().to_owned());
	Err(ControlFlow::Err(anyhow::Error::new(ExecError::ViewReadsItself {
		view: view.as_str().to_owned(),
		through: through.collect(),
	})))
}

/// `exec`, the context a view row's order runs in, with the computation-depth
/// budget spent for `levels` view rows in flight one within another, the row
/// itself the innermost: each is a write within the write that fed it, as an
/// event's write is within its record's, and spends what a write statement does
/// ([`STATEMENT_DEPTH`]). A chain of views deeper than the budget covers fails
/// with [`ExecError::ComputationDepthExceeded`] rather than exhausting the
/// stack, which every level holds frames of while the levels within it run.
///
/// Events fired by the innermost row's write start from what is left, so a
/// chain of writes that passes through views and events alike spends one
/// budget.
pub(in crate::exec::vm) fn view_row_exec(
	mut exec: ExecutionContext,
	levels: usize,
) -> FlowResult<ExecutionContext> {
	// A dive spends at most `u8::MAX` of a budget that can hold more; spending
	// the levels' cost in as few dives fails exactly where one per level would.
	let mut cost = levels.saturating_mul(usize::from(STATEMENT_DEPTH));
	while cost > 0 {
		let step = u8::try_from(cost).unwrap_or(u8::MAX);
		exec = exec.dive_by(step).map_err(ControlFlow::Err)?;
		cost -= usize::from(step);
	}
	Ok(exec)
}

/// Run a row the maintenance wrote or removed on its view table through the
/// view table's order for `action`: a write of its own, over a write state
/// stacked on the writer's, in the view table's view-maintenance context, with
/// the row's table added to the view tables in flight beneath it and the
/// budget spent for each of them ([`view_row_exec`]). The row is not one of the
/// statement's records, so nothing it does is counted towards what the
/// statement affected.
async fn run_view_row(
	m: &Maintain<'_>,
	state: &mut State,
	rows: &ViewRows,
	id: RecordId,
	action: Action,
	initial: Option<&Record>,
	current: Option<&Record>,
) -> FlowResult<()> {
	let ctx = m.scope.write_ctx_for(&id.table, WritePurpose::ViewMaintenance).await?;
	let maintaining: Arc<[TableName]> =
		m.maintaining.iter().cloned().chain(std::iter::once(id.table.clone())).collect();
	let exec = view_row_exec(ctx.exec().clone(), maintaining.len())?;
	let row = WriteRow::for_view_row(id, initial, current);
	let units = unit_table(m.program, state, &exec)?;
	let program = Arc::clone(admitted_unit(&units, rows.unit, &exec)?);
	let program = &program;
	let pipeline = program
		.pipelines
		.get(action_pipeline(action))
		.ok_or_else(|| internal("a view table's order the program does not hold"))?;
	let mut nested = State::new(program, Vec::new(), exec.function_registry())?;
	nested.frames = program.permissions.iter().map(|spec| spec.context(&exec)).collect();
	nested.permissions_bypassed = state.permissions_bypassed;
	nested.rowless = true;
	// The ids the order names are the statement's.
	nested.units = Some(Arc::clone(&units));
	nested.write =
		Some(Box::new(WriteState::view_row(Arc::clone(&m.scope), row, ctx, maintaining)));
	Box::pin(super::super::host::run_value_row(program, pipeline, Value::None, &mut nested, &exec))
		.await?;
	Ok(())
}
