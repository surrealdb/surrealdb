//! The nine per-record orders, and the control flow that selects between them.
//!
//! Six statement forms produce nine per-record orders: `UPSERT`, `RELATE` and
//! `INSERT` each have a creating and an updating order. Which one runs is
//! decided per record, by whether the record already exists. The orders differ
//! in which stages they hold, in what a shared stage acts on, and in where a
//! stage sits. Each is written out in full rather than derived from a shared
//! sequence, because the security properties live in the order itself, and a
//! shared sequence with per-form edits is easy to rebuild wrongly.
//!
//! The retry machinery is **not** a stage. `UPSERT` and `INSERT` attempt the
//! creating order behind a savepoint and reclassify a failure as an update;
//! `UPSERT` reads a record id it names first, so only a subject its source did
//! not find stored makes that attempt, and a stored one takes the updating
//! order directly. `RELATE` decides up front from the loaded record and pushes
//! no savepoint; `CREATE`, `UPDATE` and `DELETE` have no selection at all. That is this
//! module's [`Branching`], and it is why at most one record may be in flight
//! per transaction: savepoints are a stack with no handle, so a second record
//! entering before the first has left rolls back the wrong work.

// The orders and the stage registry both match on `StageId`; a wildcard arm in
// either lets a stage be added with nowhere to run, or run with no order
// naming it.
#![deny(clippy::wildcard_enum_match_arm)]

use super::ctx::TableExistence;
use super::row::WriteRow;
use super::stage::{LightweightReason, RecordImage, StageId, TableTypeFor};
use crate::doc::{Action, Error as DocError};
use crate::val::RecordId;

/// The statement form a write operator runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WriteForm {
	Create,
	Update,
	Upsert,
	Delete,
	Relate,
	Insert,
	/// A row a view's maintenance computed on its view table: written or
	/// deleted by the maintenance, which settles the row's action, and then
	/// taken through the view table's own write order. It has no statement,
	/// so no clauses, no `RETURN` and no gate.
	View,
}

impl WriteForm {
	/// The statement keyword, for `EXPLAIN` and diagnostics.
	pub(crate) fn kind(self) -> &'static str {
		match self {
			WriteForm::Create => "CREATE",
			WriteForm::Update => "UPDATE",
			WriteForm::Upsert => "UPSERT",
			WriteForm::Delete => "DELETE",
			WriteForm::Relate => "RELATE",
			WriteForm::Insert => "INSERT",
			WriteForm::View => "VIEW",
		}
	}

	/// The table `PERMISSIONS` clause that gates this form's subjects, picked by
	/// statement kind: `DELETE` by `delete`, `CREATE` by `create`,
	/// and every other mutation by `update`. A view row is gated by nothing;
	/// its maintenance writes it whatever the writer may do to the view table,
	/// and the `update` clause it answers is never asked.
	pub(crate) fn permission_kind(self) -> crate::catalog::PermissionKind {
		use crate::catalog::PermissionKind;
		match self {
			WriteForm::Create => PermissionKind::Create,
			WriteForm::Delete => PermissionKind::Delete,
			WriteForm::Update
			| WriteForm::Upsert
			| WriteForm::Relate
			| WriteForm::Insert
			| WriteForm::View => PermissionKind::Update,
		}
	}

	/// Whether the form's table must already exist. The forms that read before
	/// they write name a missing table; the forms that create define it, as
	/// a view row's maintenance defines its view table.
	pub(crate) fn table_existence(self) -> TableExistence {
		match self {
			WriteForm::Update | WriteForm::Delete => TableExistence::Required,
			WriteForm::Create
			| WriteForm::Upsert
			| WriteForm::Relate
			| WriteForm::Insert
			| WriteForm::View => TableExistence::DefinedOnDemand,
		}
	}
}

/// One of the nine per-record orders a statement runs, or one of the three a
/// write runs over a row it synthesised on a view table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WriteOrder {
	Create,
	Update,
	Delete,
	UpsertCreate,
	UpsertUpdate,
	RelateCreate,
	RelateUpdate,
	InsertCreate,
	InsertUpdate,
	/// The maintenance a view row gets after the write that produced it: the
	/// view table's own indexes, views, events, subscriptions and change feed.
	/// One per action, because the action is decided per row rather than by
	/// the statement.
	ViewCreate,
	ViewUpdate,
	ViewDelete,
}

impl WriteOrder {
	/// The view-maintenance order for one action.
	pub(crate) fn for_view(action: Action) -> Self {
		match action {
			Action::Create => WriteOrder::ViewCreate,
			Action::Update => WriteOrder::ViewUpdate,
			Action::Delete => WriteOrder::ViewDelete,
		}
	}
}

/// What the statement itself contributes to the shape of its orders.
///
/// Read once, when the orders are assembled. Nothing here is consulted per
/// record: a difference a record decides is a branch in [`Branching`], and a
/// difference the statement decides is membership in a list.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WriteFacts {
	pub(crate) form: WriteForm,
	/// Whether the statement carries a data clause. `RELATE`'s orders are the
	/// only ones that read it: a lightweight edge is admitted only without one,
	/// so the guard is in them only when the statement has data to store.
	/// `INSERT` carries none — its rows are its source rather than a clause —
	/// and its two orders ask nothing of this.
	pub(crate) has_data: bool,
	/// `INSERT … ON DUPLICATE KEY UPDATE`: without it an `INSERT` conflict is
	/// not retried as an update at all.
	pub(crate) on_duplicate_key_update: bool,
	/// `INSERT IGNORE`: a duplicate-record or unique-index conflict this form
	/// does not retry is skipped rather than raised.
	pub(crate) ignore: bool,
	/// Whether a schema failure of `UPSERT`'s create attempt may be retried as
	/// an update carrying the original failure. True for an `UPSERT` that has a
	/// data clause other than `CONTENT` or `REPLACE` and no condition — so
	/// `SET`, `MERGE`, `PATCH` and `UNSET` qualify, and a clause that replaces
	/// the record whole does not, needing nothing the store holds. False for
	/// every other form, and for an `UPSERT` with no data clause at all.
	pub(crate) repeatable: bool,
}

/// How a form selects between its orders, and what it does when a creating
/// attempt fails.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Branching {
	/// One order, no selection: `CREATE`, `UPDATE`, `DELETE`.
	Sole(WriteOrder),
	/// The order is chosen from the loaded record before any order-specific
	/// stage runs, and a failure is a failure. No savepoint, because there is
	/// no attempt to undo: `RELATE`.
	///
	/// A subject that named an id is read as part of the selection
	/// ([`WriteRow::load_named_subject`]), which is what
	/// `Processor::process_relatable` does for a `RELATE` whose edge is a
	/// record id: an edge that already exists is updated, and one that does
	/// not is created. A subject naming only a table has nothing to read and
	/// is always created.
	UpFront {
		creating: WriteOrder,
		updating: WriteOrder,
	},
	/// The creating order is attempted behind a savepoint and a classified
	/// conflict is rolled back and re-run as an update: `UPSERT`, `INSERT`.
	Retry {
		creating: WriteOrder,
		updating: WriteOrder,
		policy: RetryPolicy,
	},
	/// The order is the one for the action the caller settled, which it
	/// settled by writing or deleting the row itself: a view row.
	ByAction,
}

/// How a failed creating attempt is handled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RetryPolicy {
	/// How many stages of the creating order run *before* the savepoint is
	/// pushed: the tenant guard, which a retry re-runs as a no-op. Everything
	/// else, the id derivation included, is part of the attempt; what the
	/// derivation drew from a sequence the transaction defined is drawn again
	/// when the record goes on under that id (`WriteRow::replay_id_draws`).
	pub(crate) savepoint_after: usize,
	/// Whether a failure naming an existing record may be re-run as an update.
	/// False for `INSERT` without `ON DUPLICATE KEY UPDATE`, which has no
	/// updating clause to run.
	pub(crate) retry_conflict: bool,
	/// Whether a conflict this policy does not retry is skipped rather than
	/// raised. True for `INSERT IGNORE`, and for it alone. Only a conflict is
	/// skipped — a duplicate record id or a unique-index entry naming another
	/// record ([`is_conflict`]). Every other create failure, such as a field
	/// coercion, an `ASSERT`, a `VALUE` clause or an event that throws, is
	/// raised: the row was rejected, not found to exist already.
	pub(crate) skip_failures: bool,
	/// Whether a schema failure re-runs as an update carrying the original
	/// failure, which is re-raised if the update fails too.
	pub(crate) carry_schema_failure: bool,
}

/// A form's orders and its branching, as data.
#[derive(Clone, Debug)]
pub(crate) struct WritePlan {
	orders: Vec<(WriteOrder, Vec<StageId>)>,
	branching: Branching,
}

impl WritePlan {
	/// Assemble the orders one statement runs.
	pub(crate) fn build(facts: WriteFacts) -> Self {
		let (orders, branching) = match facts.form {
			WriteForm::Create => {
				(vec![(WriteOrder::Create, create_order())], Branching::Sole(WriteOrder::Create))
			}
			WriteForm::Update => {
				(vec![(WriteOrder::Update, update_order())], Branching::Sole(WriteOrder::Update))
			}
			WriteForm::Delete => {
				(vec![(WriteOrder::Delete, delete_order())], Branching::Sole(WriteOrder::Delete))
			}
			WriteForm::Upsert => (
				vec![
					(WriteOrder::UpsertCreate, upsert_create_order()),
					(WriteOrder::UpsertUpdate, upsert_update_order()),
				],
				Branching::Retry {
					creating: WriteOrder::UpsertCreate,
					updating: WriteOrder::UpsertUpdate,
					policy: RetryPolicy {
						// The tenant guard runs outside the savepoint; the
						// create attempt proper begins at the table-type check.
						savepoint_after: 1,
						retry_conflict: true,
						skip_failures: false,
						carry_schema_failure: facts.repeatable,
					},
				},
			),
			WriteForm::Relate => (
				vec![
					(WriteOrder::RelateCreate, relate_create_order(facts)),
					(WriteOrder::RelateUpdate, relate_update_order(facts)),
				],
				Branching::UpFront {
					creating: WriteOrder::RelateCreate,
					updating: WriteOrder::RelateUpdate,
				},
			),
			WriteForm::Insert => (
				vec![
					(WriteOrder::InsertCreate, insert_create_order()),
					(WriteOrder::InsertUpdate, insert_update_order()),
				],
				Branching::Retry {
					creating: WriteOrder::InsertCreate,
					updating: WriteOrder::InsertUpdate,
					policy: RetryPolicy {
						// The tenant guard runs outside the savepoint; the
						// create attempt proper begins at the id derivation.
						savepoint_after: 1,
						retry_conflict: facts.on_duplicate_key_update,
						skip_failures: facts.ignore,
						carry_schema_failure: false,
					},
				},
			),
			WriteForm::View => (
				[Action::Create, Action::Update, Action::Delete]
					.into_iter()
					.map(|action| (WriteOrder::for_view(action), view_maintenance_order(action)))
					.collect(),
				Branching::ByAction,
			),
		};
		Self {
			orders,
			branching,
		}
	}

	/// How the orders are selected between.
	pub(crate) fn branching(&self) -> Branching {
		self.branching
	}

	/// The named order, when this form has one.
	pub(crate) fn order(&self, which: WriteOrder) -> Option<&[StageId]> {
		self.orders.iter().find(|(o, _)| *o == which).map(|(_, s)| s.as_slice())
	}
}

/// The record a create failure says already holds the record's id, when the
/// failure is a duplicate id rather than a unique-index conflict.
pub(crate) fn duplicate_record(err: &anyhow::Error) -> Option<RecordId> {
	match err.downcast_ref::<DocError>() {
		Some(DocError::RecordExists {
			record,
		}) => Some(record.clone()),
		_ => None,
	}
}

/// What a rolled-back creating attempt becomes.
pub(crate) enum CreateFailure {
	/// Run the updating order against this record, carrying the original
	/// failure when it is one the update is expected to resolve.
	///
	/// `cause` is the conflict itself, which the retry raises if the record it
	/// names is not there once the attempt is rolled back: a conflict with a
	/// record the rolled-back attempt had itself written leaves nothing to update, and
	/// an update of nothing writes nothing and reports nothing.
	RetryAsUpdate {
		record: RecordId,
		carried: Option<anyhow::Error>,
		cause: Option<anyhow::Error>,
	},
	/// Abandon the record; the statement succeeds.
	Skip,
	/// Abandon the statement.
	Raise(anyhow::Error),
}

/// Decide what a failed creating attempt becomes.
///
/// Only two failures name a record an update can be run against: a unique-index
/// conflict, raised by the index layer, and a duplicate record id, raised by the
/// document layer. Each is recovered on its own because neither can see the
/// other's type. A conflict against an id the statement named itself is not
/// retried — the caller asked for that record specifically — and is reported
/// like any other create failure.
pub(crate) fn classify_create_failure(
	err: anyhow::Error,
	row: &WriteRow,
	policy: RetryPolicy,
) -> CreateFailure {
	if !policy.retry_conflict {
		return unretried(err, policy);
	}
	if let Some(record) = crate::idx::index_exists_record(&err) {
		return if row.is_specific_record_id() {
			unretried(err, policy)
		} else {
			CreateFailure::RetryAsUpdate {
				record,
				carried: None,
				cause: Some(err),
			}
		};
	}
	if let Some(DocError::RecordExists {
		record,
	}) = err.downcast_ref::<DocError>()
	{
		return CreateFailure::RetryAsUpdate {
			record: record.clone(),
			carried: None,
			cause: Some(err),
		};
	}
	// A schema failure is retried against the record the attempt was building,
	// because the stored record the update reads may satisfy the clause the
	// attempt's partial record did not. A record id an `UPSERT` names was read
	// before the attempt, so the record this finds stored is one whose id the
	// attempt derived itself: from a table target's data clause, or from the
	// `id` field's `DEFAULT`.
	if policy.carry_schema_failure
		&& err.downcast_ref::<DocError>().is_some_and(DocError::is_schema_related)
		&& let Some(id) = row.id.clone()
	{
		return CreateFailure::RetryAsUpdate {
			record: id,
			carried: Some(err),
			cause: None,
		};
	}
	unretried(err, policy)
}

/// A create failure this policy does not retry: skipped under `INSERT IGNORE`
/// when it is a conflict, and raised otherwise.
fn unretried(err: anyhow::Error, policy: RetryPolicy) -> CreateFailure {
	if policy.skip_failures && is_conflict(&err) {
		CreateFailure::Skip
	} else {
		CreateFailure::Raise(err)
	}
}

/// Whether a create failure is a conflict with a record that already exists: a
/// duplicate record id, raised by the document layer, or a unique-index entry,
/// raised by the index layer. These are the failures `INSERT IGNORE` skips.
fn is_conflict(err: &anyhow::Error) -> bool {
	crate::idx::index_exists_record(err).is_some()
		|| matches!(err.downcast_ref::<DocError>(), Some(DocError::RecordExists { .. }))
}

// ===========================================================================
// The nine orders
//
// The tenant guard heads all nine, and repeating it at the head of both orders of a branching
// form rather than hoisting it keeps the security property visible in the order
// itself.
//
// The selection comes first and the guard heads whichever order it picks. The
// two agree because the selection is a pure read of the already-loaded record
// — `is_new` or `is_iteration_initial` — which the guard could not change and
// which discloses nothing the caller did not supply. The guard reads only the
// caller's identity and the record's table, so a retry re-running it is a
// no-op.
// ===========================================================================

/// The `CREATE` order.
fn create_order() -> Vec<StageId> {
	vec![
		StageId::CheckRecordUserAccess,
		StageId::CheckPermissionsQuickCreate,
		StageId::CheckTableNotView,
		StageId::ComputeInputData,
		StageId::ProcessRecordData,
		StageId::GenerateRecordId,
		StageId::CheckTableType(TableTypeFor::Create),
		StageId::CheckDataFields,
		StageId::DefaultRecordData,
		StageId::ProcessTableFields,
		StageId::CleanupTableFields,
		StageId::CheckCreatePermissions,
		StageId::StoreRecordData,
		StageId::StoreIndexData,
		StageId::MaterialiseObservedFields,
		StageId::ProcessTableReferences,
		StageId::ProcessTableViews(Action::Create),
		StageId::ProcessTableEvents(Action::Create),
		StageId::ProcessTableLives(Action::Create),
		StageId::ProcessChangefeeds,
		StageId::CheckSelectPermissions(RecordImage::Current),
		StageId::OutputWrite,
	]
}

/// The `UPDATE` order.
fn update_order() -> Vec<StageId> {
	vec![
		StageId::CheckRecordUserAccess,
		StageId::CheckRecordExists,
		StageId::CheckUpdatePermissions,
		StageId::CheckTableNotView,
		StageId::CheckTableNotLightweight(LightweightReason::Updated),
		StageId::CheckWhereCondition,
		StageId::ComputeInputData,
		StageId::CheckDataFields,
		StageId::ProcessRecordData,
		StageId::DefaultRecordData,
		StageId::ProcessTableFields,
		StageId::CleanupTableFields,
		StageId::RecheckUpdatePermissions,
		StageId::StoreRecordData,
		StageId::StoreInlineAdjacencyData,
		StageId::StoreIndexData,
		StageId::MaterialiseObservedFields,
		StageId::ProcessTableReferences,
		StageId::ProcessTableViews(Action::Update),
		StageId::ProcessTableEvents(Action::Update),
		StageId::ProcessTableLives(Action::Update),
		StageId::ProcessChangefeeds,
		StageId::CheckSelectPermissions(RecordImage::Current),
		StageId::OutputWrite,
	]
}

/// The maintenance a row this write synthesised on a view table gets.
///
/// A view row is a record on a real table: it is indexed, it can be the source
/// of a second-order view, it is watched, its table's events fire for it, and
/// it appears in a change feed. What it does *not* get is the front half of a
/// statement's order — no tenant guard, no permission gate, no condition, no
/// data clause and no field validation. The row was computed by the view's own
/// clauses from a record the statement already wrote; there is no caller
/// subject to confine, no user-supplied content to validate, and re-running
/// the field pipeline would evaluate every view expression a second time.
///
/// `view_row_reaches_nothing` (`stages/observe.rs`) answers whether these stages
/// write anything for a table, and the retirement of a view's rows deletes them
/// as one range on its answer, so a stage added here is added to its answer too.
fn view_maintenance_order(action: Action) -> Vec<StageId> {
	vec![
		StageId::StoreIndexData,
		StageId::ProcessTableViews(action),
		StageId::ProcessTableEvents(action),
		StageId::ProcessTableLives(action),
		StageId::ProcessChangefeeds,
	]
}

/// The `DELETE` order. Its own shape: no data clause, reference
/// cleanup before the clear because the cleanup walks the image the clear
/// blanks, and the doc-id reclaim between the index maintenance that reports it
/// and the purge.
fn delete_order() -> Vec<StageId> {
	vec![
		StageId::CheckRecordUserAccess,
		StageId::CheckRecordExists,
		StageId::CheckDeletePermissions,
		StageId::CheckTableNotView,
		StageId::CheckWhereCondition,
		StageId::CleanupTableReferences,
		StageId::ClearRecordData,
		StageId::StoreIndexData,
		StageId::ReclaimDocId,
		StageId::PurgeRecordData,
		StageId::MaterialiseObservedFields,
		StageId::ProcessTableViews(Action::Delete),
		StageId::ProcessTableEvents(Action::Delete),
		StageId::ProcessTableLives(Action::Delete),
		StageId::ProcessChangefeeds,
		StageId::CheckSelectPermissions(RecordImage::Initial),
		StageId::OutputWrite,
	]
}

/// The `UPSERT` create order. The table-type check
/// precedes the permission gate here, so a caller denied `create` is told the
/// table's type rather than silently skipped.
fn upsert_create_order() -> Vec<StageId> {
	vec![
		StageId::CheckRecordUserAccess,
		StageId::CheckTableType(TableTypeFor::Upsert),
		StageId::CheckPermissionsQuickCreate,
		StageId::CheckTableNotView,
		StageId::ComputeInputData,
		StageId::ProcessRecordData,
		StageId::GenerateRecordId,
		StageId::CheckDataFields,
		StageId::DefaultRecordData,
		StageId::ProcessTableFields,
		StageId::CleanupTableFields,
		StageId::CheckCreatePermissions,
		StageId::StoreRecordData,
		StageId::StoreIndexData,
		StageId::MaterialiseObservedFields,
		StageId::ProcessTableReferences,
		StageId::ProcessTableViews(Action::Create),
		StageId::ProcessTableEvents(Action::Create),
		StageId::ProcessTableLives(Action::Create),
		StageId::ProcessChangefeeds,
		StageId::CheckSelectPermissions(RecordImage::Current),
		StageId::OutputWrite,
	]
}

/// The `UPSERT` update order. Carries no lightweight
/// guard: `UPDATE`'s is unconditional, and this order has none.
fn upsert_update_order() -> Vec<StageId> {
	vec![
		StageId::CheckRecordUserAccess,
		StageId::CheckRecordExists,
		StageId::CheckTableType(TableTypeFor::Upsert),
		StageId::CheckUpdatePermissions,
		StageId::CheckTableNotView,
		StageId::CheckWhereCondition,
		StageId::ComputeInputData,
		StageId::CheckDataFields,
		StageId::ProcessRecordData,
		StageId::DefaultRecordData,
		StageId::ProcessTableFields,
		StageId::CleanupTableFields,
		StageId::RecheckUpdatePermissions,
		StageId::StoreRecordData,
		StageId::StoreInlineAdjacencyData,
		StageId::StoreIndexData,
		StageId::MaterialiseObservedFields,
		StageId::ProcessTableReferences,
		StageId::ProcessTableViews(Action::Update),
		StageId::ProcessTableEvents(Action::Update),
		StageId::ProcessTableLives(Action::Update),
		StageId::ProcessChangefeeds,
		StageId::CheckSelectPermissions(RecordImage::Current),
		StageId::OutputWrite,
	]
}

/// The `RELATE` create order.
fn relate_create_order(facts: WriteFacts) -> Vec<StageId> {
	let mut order = vec![
		StageId::CheckRecordUserAccess,
		StageId::CheckPermissionsQuickCreate,
		StageId::CheckTableNotView,
		StageId::ComputeInputData,
		StageId::ProcessRecordData,
		StageId::GenerateRecordId,
		StageId::CheckTableType(TableTypeFor::Relate),
	];
	if facts.has_data {
		order.push(StageId::CheckTableNotLightweight(LightweightReason::GivenADataClause));
	}
	order.extend([
		StageId::CheckDataFields,
		StageId::DefaultRecordData,
		StageId::ProcessTableFields,
		StageId::CleanupTableFields,
		StageId::CheckCreatePermissions,
		StageId::StoreRecordData,
		StageId::StoreEdgesData,
		StageId::StoreIndexData,
		StageId::MaterialiseObservedFields,
		StageId::ProcessTableReferences,
		StageId::ProcessTableViews(Action::Create),
		StageId::ProcessTableEvents(Action::Create),
		StageId::ProcessTableLives(Action::Create),
		StageId::ProcessChangefeeds,
		StageId::CheckSelectPermissions(RecordImage::Current),
		StageId::OutputWrite,
	]);
	order
}

/// The `RELATE` update order. The one updating order
/// that stores no inline adjacency.
fn relate_update_order(facts: WriteFacts) -> Vec<StageId> {
	let mut order = vec![
		StageId::CheckRecordUserAccess,
		StageId::CheckRecordExists,
		StageId::CheckTableType(TableTypeFor::Relate),
		StageId::CheckUpdatePermissions,
		StageId::CheckTableNotView,
	];
	if facts.has_data {
		order.push(StageId::CheckTableNotLightweight(LightweightReason::GivenADataClause));
	}
	order.extend([
		StageId::ComputeInputData,
		StageId::CheckDataFields,
		StageId::ProcessRecordData,
		StageId::DefaultRecordData,
		StageId::ProcessTableFields,
		StageId::CleanupTableFields,
		StageId::RecheckUpdatePermissions,
		StageId::StoreRecordData,
		StageId::StoreEdgesData,
		StageId::StoreIndexData,
		StageId::MaterialiseObservedFields,
		StageId::ProcessTableReferences,
		StageId::ProcessTableViews(Action::Update),
		StageId::ProcessTableEvents(Action::Update),
		StageId::ProcessTableLives(Action::Update),
		StageId::ProcessChangefeeds,
		StageId::CheckSelectPermissions(RecordImage::Current),
		StageId::OutputWrite,
	]);
	order
}

/// The `INSERT` create order, which derives the record's id before checking
/// the table type. It is the only creating order that applies its data by
/// merging the row's own value, and the only one that evaluates no data clause:
/// an `INSERT`'s one clause is `ON DUPLICATE KEY UPDATE`, which belongs to the
/// updating order and reads the stored record a collision names, so a row that
/// collides with nothing never evaluates it. It also validates the special
/// fields before applying the data, as the four updating orders do. The other
/// creating orders skip that validation.
fn insert_create_order() -> Vec<StageId> {
	vec![
		StageId::CheckRecordUserAccess,
		StageId::GenerateRecordId,
		StageId::CheckTableType(TableTypeFor::Insert),
		StageId::CheckPermissionsQuickCreate,
		StageId::CheckTableNotView,
		StageId::CheckDataFields,
		StageId::ProcessMergeData,
		StageId::DefaultRecordData,
		StageId::ProcessTableFields,
		StageId::CleanupTableFields,
		StageId::CheckCreatePermissions,
		StageId::StoreRecordData,
		StageId::StoreEdgesData,
		StageId::StoreIndexData,
		StageId::MaterialiseObservedFields,
		StageId::ProcessTableReferences,
		StageId::ProcessTableViews(Action::Create),
		StageId::ProcessTableEvents(Action::Create),
		StageId::ProcessTableLives(Action::Create),
		StageId::ProcessChangefeeds,
		StageId::CheckSelectPermissions(RecordImage::Current),
		StageId::OutputWrite,
	]
}

/// The `INSERT` update order. It has no condition, and
/// stores both the edge keys and the inline adjacency.
fn insert_update_order() -> Vec<StageId> {
	vec![
		StageId::CheckRecordUserAccess,
		StageId::CheckRecordExists,
		StageId::CheckTableType(TableTypeFor::Insert),
		StageId::CheckUpdatePermissions,
		StageId::CheckTableNotView,
		StageId::ComputeInputData,
		StageId::CheckDataFields,
		StageId::ProcessRecordData,
		StageId::DefaultRecordData,
		StageId::ProcessTableFields,
		StageId::CleanupTableFields,
		StageId::RecheckUpdatePermissions,
		StageId::StoreRecordData,
		StageId::StoreEdgesData,
		StageId::StoreInlineAdjacencyData,
		StageId::StoreIndexData,
		StageId::MaterialiseObservedFields,
		StageId::ProcessTableReferences,
		StageId::ProcessTableViews(Action::Update),
		StageId::ProcessTableEvents(Action::Update),
		StageId::ProcessTableLives(Action::Update),
		StageId::ProcessChangefeeds,
		StageId::CheckSelectPermissions(RecordImage::Current),
		StageId::OutputWrite,
	]
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The stages of a view row's order are those `view_row_reaches_nothing`
	/// (`stages/observe.rs`) answers for, which a view's retirement relies on to
	/// delete the rows of a table they write nothing for as one range.
	#[test]
	fn a_view_rows_order_is_the_stages_view_row_reaches_nothing_answers_for() {
		for action in [Action::Create, Action::Update, Action::Delete] {
			assert_eq!(
				view_maintenance_order(action),
				[
					StageId::StoreIndexData,
					StageId::ProcessTableViews(action),
					StageId::ProcessTableEvents(action),
					StageId::ProcessTableLives(action),
					StageId::ProcessChangefeeds,
				],
				"a stage added to a view row's order is added to `view_row_reaches_nothing`"
			);
		}
	}
}
