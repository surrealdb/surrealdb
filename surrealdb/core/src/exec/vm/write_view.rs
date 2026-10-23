//! The host's side of a table's views maintained as instructions.
//!
//! A write keeps every view its table feeds in step with the record, as the
//! views stage does: a materialized view holds one row per source record under
//! the record's own key, or under the key its projected `id` gives, and an
//! aggregated view one row per group, whose stats
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

use surrealdb_types::ToSql as _;

use super::data::PlaceRun;
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
	Group, GroupSlots, Slot, ViewBatch, changed_no_row, group_values, left_no_row, new_stats,
	no_record_count, row_of,
};
use crate::exec::permission::view_frame;
use crate::exec::vm::depth::STATEMENT_DEPTH;
use crate::exec::vm::host::{admitted_unit, gather_program, unit_table};
use crate::exec::vm::program::{PlaceStepSpec, Program, UnitId};
use crate::exec::{Error as ExecError, ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::expr::paths::ID;
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
		/// Whether the projection names the row's `id` (`... AS id`): the row
		/// is then held under the key its `id` gives ([`view_row_key`]), and
		/// otherwise under the source record's key.
		keyed: bool,
		rows: ViewRows,
		/// Whether a clause of the view may read stored data.
		reads_data: bool,
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
		/// Whether a clause of the view may read stored data.
		reads_data: bool,
	},
}

impl LoweredView {
	/// Whether a clause of the view may read stored data, such as a row
	/// another view of the same table holds, so that what the views maintained
	/// before it made of the record is written before it is evaluated.
	pub(in crate::exec::vm) fn reads_data(&self) -> bool {
		match self {
			LoweredView::Unmaintained => false,
			LoweredView::Materialized {
				reads_data,
				..
			}
			| LoweredView::Aggregated {
				reads_data,
				..
			} => *reads_data,
		}
	}
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
	// A view's clauses read a record's `id` as a select over its table does,
	// and a view row's images carry none, its identity being its key.
	let initial = with_id(row.initial.clone(), &id);
	let current = with_id(row.current.clone(), &id);
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

/// A row of a view table the maintenance wrote or removed, which the view
/// table's order runs over once everything the record touched is written.
struct RowChange<'a> {
	rows: &'a ViewRows,
	id: RecordId,
	action: Action,
	before: Option<Record>,
	after: Option<Record>,
	/// Whether the group's statistics moved. A row holding no statistics
	/// still counts as modified when they did.
	state_moved: bool,
}

/// One view's part of a record's maintenance, planned before storage is
/// read.
enum Planned<'a> {
	Materialized {
		view: Materialized<'a>,
		/// The row the record made before the write, which is removed: the
		/// record left the view or its row moved to another key.
		removal: Option<(RecordIdKey, Slot)>,
		/// The row the write makes, with its data.
		write: Option<(RecordIdKey, Value, Slot)>,
	},
	Aggregated {
		view: Aggregated<'a>,
		moves: Moves,
	},
}

/// A group of an aggregated view, with the keys its row and state are read
/// from.
struct Located {
	key: RecordIdKey,
	slots: GroupSlots,
}

/// How a record's change moves it between the groups of one aggregated view.
enum Moves {
	/// The record is in no group before or after.
	Nothing,
	/// The record joined the group.
	Joined(Located),
	/// The record left the group.
	Left(Located),
	/// The record changed within the group.
	Within(Located),
	/// The record left the first group and joined the second.
	Moved(Located, Located),
}

/// The image `image` of the record `id`, with its `id` field set when it is an
/// object that holds none.
fn with_id(mut image: Value, id: &RecordId) -> Value {
	if let Value::Object(object) = &mut image
		&& !object.contains_key("id")
	{
		object.insert("id", Value::RecordId(id.clone()));
	}
	image
}

/// Maintain each of `lowered`'s views for the record `id` whose images are
/// `initial` and `current`. A view whose row is already in flight beneath the
/// record's write fails it before anything of it is evaluated
/// ([`refuse_reentry`]).
///
/// The views are maintained in runs. Within a run, the keys every view will
/// read are named first, from the images alone, and read with one multi-get;
/// each view's change is then applied in memory, what it wrote is stored with
/// one multi-write, and last each row written or removed runs through its view
/// table's order, as a write of its own. A view whose clauses may read stored
/// data ([`LoweredView::reads_data`]) starts a run of its own, so that it reads
/// what the views before it made of the record, as it would were each view
/// maintained on its own.
async fn maintain(
	m: &Maintain<'_>,
	lowered: &LoweredViews,
	state: &mut State,
	id: &RecordId,
	initial: &Value,
	current: &Value,
	action: Action,
) -> FlowResult<()> {
	let mut batch = ViewBatch::new(&m.ctx);
	let mut planned = Vec::with_capacity(lowered.views.len());
	for (definition, view) in lowered.definitions.iter().zip(&lowered.views) {
		if matches!(view, LoweredView::Unmaintained) {
			continue;
		}
		refuse_reentry(&m.maintaining, &definition.name)?;
		if view.reads_data() && !planned.is_empty() {
			let run = std::mem::take(&mut planned);
			settle(m, state, &mut batch, run, initial, current).await?;
		}
		planned.push(match view {
			LoweredView::Unmaintained => continue,
			LoweredView::Materialized {
				condition,
				projection,
				keyed,
				rows,
				..
			} => {
				let view = Materialized {
					table: &definition.name,
					condition: *condition,
					projection: *projection,
					keyed: *keyed,
					rows,
				};
				let plan = plan_materialized(m, state, &view, id, initial, current, action).await?;
				let removal = match plan.removal {
					Some(key) => {
						let slot = batch.want_row(view.table, &key)?;
						Some((key, slot))
					}
					None => None,
				};
				let write = match plan.write {
					Some((key, data)) => {
						let slot = batch.want_row(view.table, &key)?;
						Some((key, data, slot))
					}
					None => None,
				};
				Planned::Materialized {
					view,
					removal,
					write,
				}
			}
			LoweredView::Aggregated {
				condition,
				group,
				arguments,
				fields,
				recalc,
				rows,
				..
			} => {
				let Some(ViewDefinition::Aggregated {
					analysis,
					..
				}) = &definition.view
				else {
					return Err(internal("an aggregated view whose definition is not one"));
				};
				let view = Aggregated {
					analysis,
					table: &definition.name,
					condition: *condition,
					group,
					arguments,
					fields,
					recalc: recalc.as_ref(),
					rows,
				};
				let moves = moves_of(m, state, &view, &mut batch, initial, current, action).await?;
				Planned::Aggregated {
					view,
					moves,
				}
			}
		});
	}
	settle(m, state, &mut batch, planned, initial, current).await
}

/// Maintain one run of views planned over `batch`: read the keys they named,
/// apply each view's change, write what they staged, and run each row written
/// or removed through its view table's order.
async fn settle<'a>(
	m: &Maintain<'_>,
	state: &mut State,
	batch: &mut ViewBatch,
	planned: Vec<Planned<'a>>,
	initial: &Value,
	current: &Value,
) -> FlowResult<()> {
	if planned.is_empty() {
		return Ok(());
	}
	batch.fetch(&m.ctx).await?;
	let mut changes = Vec::new();
	for planned in planned {
		match planned {
			Planned::Materialized {
				view,
				removal,
				write,
			} => {
				materialized(m, batch, &mut changes, &view, removal, write).await?;
			}
			Planned::Aggregated {
				view,
				moves,
			} => {
				aggregated(m, state, batch, &mut changes, &view, moves, initial, current).await?;
			}
		}
	}
	batch.flush(&m.ctx).await?;
	for change in changes {
		run_view_row(
			m,
			state,
			change.rows,
			change.id,
			change.action,
			change.before.as_ref(),
			change.after.as_ref(),
			change.state_moved,
		)
		.await?;
	}
	Ok(())
}

/// One materialized view's maintenance.
struct Materialized<'a> {
	table: &'a TableName,
	condition: Option<ViewClause>,
	projection: ViewClause,
	keyed: bool,
	rows: &'a ViewRows,
}

/// What a record's change does to a materialized view, decided from the
/// images alone before storage is read.
struct MaterializedPlan {
	/// The key of the row the record made before the write, when that row is
	/// to go.
	removal: Option<RecordIdKey>,
	/// The key and data of the row the write makes.
	write: Option<(RecordIdKey, Value)>,
}

/// Decide a materialized view's maintenance: the record's row is written when
/// the view's condition admits the record's post-image — every record the
/// write did not delete, for a view with none — and removed otherwise. The
/// view row's own action is what the view table held: a record the condition
/// newly admits creates its row whatever the statement did.
///
/// The row is held under the source record's key, or, for a view whose
/// projection sets the row's `id`, under the key that `id` gives. Such a
/// view's row for the record before the write is found by judging the view's
/// condition and projection over the pre-image, so an update or delete
/// evaluates them over both images, and a write that moves the row to another
/// key removes it from the old one. Records that make the same key share one
/// row: the record written last holds it, and any of them leaving the view or
/// moving to another key removes it.
async fn plan_materialized(
	m: &Maintain<'_>,
	state: &mut State,
	view: &Materialized<'_>,
	id: &RecordId,
	initial: &Value,
	current: &Value,
	action: Action,
) -> FlowResult<MaterializedPlan> {
	let admitted = match view.condition {
		Some(condition) => clause(m, condition, current, state).await?.is_truthy(),
		None => action != Action::Delete,
	};
	// The key of the row the record made before this write.
	let held = if !view.keyed {
		Some(id.key.clone())
	} else if action == Action::Create {
		None
	} else {
		let was_admitted = match view.condition {
			Some(condition) => clause(m, condition, initial, state).await?.is_truthy(),
			None => !initial.is_nullish(),
		};
		if was_admitted {
			let data = clause(m, view.projection, initial, state).await?;
			Some(view_row_key(data.pick(&ID), view.table)?)
		} else {
			None
		}
	};
	if !admitted {
		return Ok(MaterializedPlan {
			removal: held,
			write: None,
		});
	}
	let mut data = clause(m, view.projection, current, state).await?;
	let key = if view.keyed {
		view_row_key(data.pick(&ID), view.table)?
	} else {
		id.key.clone()
	};
	// The row's identity is its key: its data carries no `id`, as the backfill
	// stores it.
	if let Value::Object(object) = &mut data {
		object.remove("id");
	}
	Ok(MaterializedPlan {
		removal: held.filter(|held| *held != key),
		write: Some((key, data)),
	})
}

/// Apply a materialized view's plan to the rows read: remove the row that
/// goes, then write the row the record makes. Removing a row the view never
/// held changes nothing.
async fn materialized<'a>(
	m: &Maintain<'_>,
	batch: &mut ViewBatch,
	changes: &mut Vec<RowChange<'a>>,
	view: &Materialized<'a>,
	removal: Option<(RecordIdKey, Slot)>,
	write: Option<(RecordIdKey, Value, Slot)>,
) -> FlowResult<()> {
	let txn = m.ctx.txn();
	if let Some((key, slot)) = removal {
		let before = batch.read_row(&txn, slot, view.table, &key).await?;
		if before.is_some() {
			batch.delete_row(slot, view.table, &key);
			changes.push(RowChange {
				rows: view.rows,
				id: RecordId {
					table: view.table.clone(),
					key,
				},
				action: Action::Delete,
				before,
				after: None,
				state_moved: false,
			});
		}
	}
	if let Some((key, data, slot)) = write {
		// The row's before-image, read before it is overwritten.
		let before = batch.read_row(&txn, slot, view.table, &key).await?;
		let record = Record::new(data);
		batch.write_row(&txn, slot, view.table, &key, &record, before.is_none()).await?;
		let action = match before {
			Some(_) => Action::Update,
			None => Action::Create,
		};
		changes.push(RowChange {
			rows: view.rows,
			id: RecordId {
				table: view.table.clone(),
				key,
			},
			action,
			before,
			after: Some(record),
			state_moved: false,
		});
	}
	Ok(())
}

/// The key a materialized view's row is held under, from the `id` of the row
/// its projection made, as `CREATE` reads a record's `id` field: a record id
/// gives its key whatever its table, and a number, string, array, object or
/// UUID is the key itself. Any other `id` fails the write — `NONE`, since a
/// row with no key of its own could not be found again to update or remove it,
/// and a range, which names no one record.
pub(in crate::exec::vm) fn view_row_key(id: Value, table: &TableName) -> FlowResult<RecordIdKey> {
	let invalid = |id: &Value| {
		ControlFlow::Err(anyhow::Error::new(crate::expr::Error::IdInvalid {
			value: id.to_sql(),
		}))
	};
	if id.is_none() {
		return Err(invalid(&id));
	}
	let id = id.generate(table.clone(), true).map_err(ControlFlow::Err)?;
	if id.key.is_range() {
		return Err(invalid(&Value::RecordId(id)));
	}
	Ok(id.key)
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

/// The groups the record is in under each image, and the keys of them to
/// read. A record that moved between groups leaves one and joins the other.
///
/// The group a record leaves is derived from its before-image under the
/// definitions in force, and the view holds a row for it only if the same
/// derivation put the record there. A schema statement that changes what a
/// clause of the view derives from a record the table holds, such as redefining
/// a `COMPUTED` field the view depends on, is therefore refused
/// (`ddl::define_field_statement_validate_computed_views`).
async fn moves_of(
	m: &Maintain<'_>,
	state: &mut State,
	view: &Aggregated<'_>,
	batch: &mut ViewBatch,
	initial: &Value,
	current: &Value,
	action: Action,
) -> FlowResult<Moves> {
	let table = view.table;
	Ok(match action {
		Action::Create => match group_of(m, view, current, state).await? {
			Some(group) => Moves::Joined(locate(batch, table, group)?),
			None => Moves::Nothing,
		},
		Action::Delete => match group_of(m, view, initial, state).await? {
			Some(group) => Moves::Left(locate(batch, table, group)?),
			None => Moves::Nothing,
		},
		Action::Update => {
			let before = group_of(m, view, initial, state).await?;
			let after = group_of(m, view, current, state).await?;
			match (before, after) {
				(None, None) => Moves::Nothing,
				(Some(before), Some(after)) if before == after => {
					Moves::Within(locate(batch, table, before)?)
				}
				(Some(before), Some(after)) => {
					let from = locate(batch, table, before)?;
					let key = RecordIdKey::Array(Array(after));
					// Group values that differ but encode to one key are one group.
					if batch.is_group(from.slots, table, &key)? {
						Moves::Within(from)
					} else {
						let slots = batch.want_group(table, &key)?;
						Moves::Moved(
							from,
							Located {
								key,
								slots,
							},
						)
					}
				}
				(Some(before), None) => Moves::Left(locate(batch, table, before)?),
				(None, Some(after)) => Moves::Joined(locate(batch, table, after)?),
			}
		}
	})
}

/// The group of `table` whose values are `group`, with its keys named in
/// `batch` to read.
fn locate(batch: &mut ViewBatch, table: &TableName, group: Vec<Value>) -> FlowResult<Located> {
	let key = RecordIdKey::Array(Array(group));
	let slots = batch.want_group(table, &key)?;
	Ok(Located {
		key,
		slots,
	})
}

/// Maintain an aggregated view: the stats of the groups the record joined,
/// left or changed within adjusted by what it contributes.
#[expect(clippy::too_many_arguments, reason = "the batch, the changes and the record's images")]
async fn aggregated<'a>(
	m: &Maintain<'_>,
	state: &mut State,
	batch: &mut ViewBatch,
	changes: &mut Vec<RowChange<'a>>,
	view: &Aggregated<'a>,
	moves: Moves,
	initial: &Value,
	current: &Value,
) -> FlowResult<()> {
	match moves {
		Moves::Nothing => Ok(()),
		Moves::Joined(at) => group_create(m, state, batch, changes, view, current, at).await,
		Moves::Left(at) => group_delete(m, state, batch, changes, view, initial, at).await,
		Moves::Within(at) => {
			group_update(m, state, batch, changes, view, initial, current, at).await
		}
		Moves::Moved(from, to) => {
			group_delete(m, state, batch, changes, view, initial, from).await?;
			group_create(m, state, batch, changes, view, current, to).await
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

/// A record joined the group `at`: its arguments added to the group's stats,
/// from the analysis's own for a group no row was in yet.
async fn group_create<'a>(
	m: &Maintain<'_>,
	state: &mut State,
	batch: &mut ViewBatch,
	changes: &mut Vec<RowChange<'a>>,
	view: &Aggregated<'a>,
	current: &Value,
	at: Located,
) -> FlowResult<()> {
	let (before, mut stats, action) =
		match batch.read_group(&m.ctx.txn(), at.slots, view.table, &at.key).await? {
			Some(group) => (Some(group.row), group.stats, Action::Update),
			None => (None, new_stats(view.analysis), Action::Create),
		};
	let prior = (action == Action::Update && batch.keyed(at.slots)).then(|| stats.clone());
	let arguments = arguments_of(m, view, current, state).await?;
	add_to_aggregation_stats(&arguments, &mut stats).map_err(ControlFlow::Err)?;
	let stored = Stored {
		at,
		before,
		prior,
		action,
	};
	store_group(m, state, batch, changes, view, stored, stats).await
}

/// A record left the group `at`: its arguments taken back out of the group's
/// stats, an extreme it held recomputed over the group's other records, and
/// the row removed when it was the group's last record, except a `GROUP ALL`
/// row, which is emptied instead.
async fn group_delete<'a>(
	m: &Maintain<'_>,
	state: &mut State,
	batch: &mut ViewBatch,
	changes: &mut Vec<RowChange<'a>>,
	view: &Aggregated<'a>,
	initial: &Value,
	at: Located,
) -> FlowResult<()> {
	let table = view.table;
	let Some(group) = batch.read_group(&m.ctx.txn(), at.slots, table, &at.key).await? else {
		return Err(left_no_row(&group_values(&at.key), table));
	};
	let Group {
		row: before,
		mut stats,
	} = group;
	let Some(count) = AggregationStat::get_count(&stats) else {
		return Err(no_record_count(table));
	};
	if count == 1 && view.group.is_empty() {
		// The `GROUP ALL` row outlives its last record: it is emptied to the stats of
		// a group no record is in.
		let prior = batch.keyed(at.slots).then(|| stats.clone());
		let stored = Stored {
			at,
			before: Some(before),
			prior,
			action: Action::Update,
		};
		return store_group(m, state, batch, changes, view, stored, new_stats(view.analysis)).await;
	}
	if count == 1 {
		batch.delete_group(at.slots, table, &at.key);
		changes.push(RowChange {
			rows: view.rows,
			id: RecordId {
				table: table.clone(),
				key: at.key,
			},
			action: Action::Delete,
			before: Some(before),
			after: None,
			state_moved: false,
		});
		return Ok(());
	}
	let prior = batch.keyed(at.slots).then(|| stats.clone());
	let arguments = arguments_of(m, view, initial, state).await?;
	let recalculations =
		remove_from_aggregation_stats(&arguments, &mut stats).map_err(ControlFlow::Err)?;
	let group = group_values(&at.key);
	recalculate(m, state, view, &group, &mut stats, &recalculations).await?;
	let stored = Stored {
		at,
		before: Some(before),
		prior,
		action: Action::Update,
	};
	store_group(m, state, batch, changes, view, stored, stats).await
}

/// A record changed within the group `at`: the change between what it
/// contributed and what it contributes now applied to the group's stats.
#[expect(clippy::too_many_arguments, reason = "the batch, the changes and the record's images")]
async fn group_update<'a>(
	m: &Maintain<'_>,
	state: &mut State,
	batch: &mut ViewBatch,
	changes: &mut Vec<RowChange<'a>>,
	view: &Aggregated<'a>,
	initial: &Value,
	current: &Value,
	at: Located,
) -> FlowResult<()> {
	let Some(group) = batch.read_group(&m.ctx.txn(), at.slots, view.table, &at.key).await? else {
		return Err(changed_no_row(&group_values(&at.key), view.table));
	};
	let Group {
		row: before,
		mut stats,
	} = group;
	let prior = batch.keyed(at.slots).then(|| stats.clone());
	let was = arguments_of(m, view, initial, state).await?;
	let now = arguments_of(m, view, current, state).await?;
	let recalculations =
		update_aggregation_stats(&was, &now, &mut stats).map_err(ControlFlow::Err)?;
	let group = group_values(&at.key);
	recalculate(m, state, view, &group, &mut stats, &recalculations).await?;
	let stored = Stored {
		at,
		before: Some(before),
		prior,
		action: Action::Update,
	};
	store_group(m, state, batch, changes, view, stored, stats).await
}

/// What a group held before a change, and the row action the change is.
struct Stored {
	at: Located,
	/// The group's row as it was, `None` for a group created by the change.
	before: Option<Record>,
	/// The group's statistics as they were, kept only where the row does not
	/// carry them to compare against.
	prior: Option<Vec<AggregationStat>>,
	action: Action,
}

/// Render a group's stats into its row — each field evaluated against the
/// document the group key and stats make, and written to its place — stage
/// the row and its state, and note the row for the view table's order.
async fn store_group<'a>(
	m: &Maintain<'_>,
	state: &mut State,
	batch: &mut ViewBatch,
	changes: &mut Vec<RowChange<'a>>,
	view: &Aggregated<'a>,
	stored: Stored,
	stats: Vec<AggregationStat>,
) -> FlowResult<()> {
	let Stored {
		at,
		before,
		prior,
		action,
	} = stored;
	let document =
		Value::Object(aggregation::create_field_document(&group_values(&at.key), &stats));
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
	let keyed = batch.keyed(at.slots);
	let after = row_of(keyed, data, &stats);
	batch
		.write_group(
			&m.ctx.txn(),
			at.slots,
			view.table,
			&at.key,
			&after,
			&stats,
			action == Action::Create,
		)
		.await?;
	// A row that keeps its statistics inline moves with them; one that does
	// not is told whether they moved.
	let state_moved = keyed && (action == Action::Create || prior.as_deref() != Some(&stats[..]));
	changes.push(RowChange {
		rows: view.rows,
		id: RecordId {
			table: view.table.clone(),
			key: at.key,
		},
		action,
		before: match action {
			Action::Create => None,
			Action::Update | Action::Delete => before,
		},
		after: Some(after),
		state_moved,
	});
	Ok(())
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
	let pipeline = if let Value::Object(_) = subject {
		clause.object
	} else {
		clause.any
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
/// view table's order for `action`, `state_moved` saying whether the statistics
/// the row is computed from changed with it: a write of its own, over a write
/// state stacked on the writer's, in the view table's view-maintenance context,
/// with the row's table added to the view tables in flight beneath it and the
/// budget spent for each of them ([`view_row_exec`]). The row is not one of the
/// statement's records, so nothing it does is counted towards what the
/// statement affected.
#[expect(clippy::too_many_arguments, reason = "a row's id, action and both images")]
async fn run_view_row(
	m: &Maintain<'_>,
	state: &mut State,
	rows: &ViewRows,
	id: RecordId,
	action: Action,
	initial: Option<&Record>,
	current: Option<&Record>,
	state_moved: bool,
) -> FlowResult<()> {
	let ctx = m.scope.write_ctx_for(&id.table, WritePurpose::ViewMaintenance).await?;
	let maintaining: Arc<[TableName]> =
		m.maintaining.iter().cloned().chain(std::iter::once(id.table.clone())).collect();
	let exec = view_row_exec(ctx.exec().clone(), maintaining.len())?;
	let row = WriteRow::for_view_row(id, initial, current, state_moved);
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
