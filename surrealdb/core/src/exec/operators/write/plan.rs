//! The nine per-record orders, and the control flow that selects between them.
//!
//! Six statement forms produce nine per-record orders: `UPSERT`, `RELATE` and
//! `INSERT` each have a distinct creating and updating order, selected per
//! record by whether the record already exists. The orders differ in which
//! stages they contain, in what a shared stage acts on, and in where a stage
//! sits, so they are written out here in full rather than derived from one
//! another: the order is where the security properties live, and a shared
//! sequence with per-form edits reconstructs it differently every time it is
//! read.
//!
//! The retry machinery is **not** a stage. `UPSERT` and `INSERT` attempt the
//! creating order behind a savepoint and reclassify a failure as an update;
//! `RELATE` decides up front from the loaded record and pushes no savepoint;
//! `CREATE`, `UPDATE` and `DELETE` have no selection at all. That is this
//! module's [`Branching`], and it is why at most one record may be in flight
//! per transaction: savepoints are a stack with no handle, so a second record
//! entering before the first has left rolls back the wrong work.

// The orders and the stage registry both match on `StageId`; a wildcard arm in
// either lets a stage be added with nowhere to run, or run with no order
// naming it.
#![warn(clippy::wildcard_enum_match_arm)]

use std::sync::Arc;

use super::ctx::{TableExistence, WriteCtx};
use super::row::WriteRow;
use super::stage::{
	LightweightReason, RecordImage, StageId, StageOutcome, TableTypeFor, WriteStage,
};
use crate::catalog::providers::TableProvider;
use crate::doc::{Action, Error as DocError};
use crate::expr::ControlFlow;
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
	/// Every order a *statement* runs, so a test over the set cannot silently
	/// omit one. The three view-maintenance orders are deliberately absent:
	/// they are not a form's, they open on no tenant guard, and a property
	/// asserted of every statement order is not asserted of them.
	pub(crate) const ALL: [WriteOrder; 9] = [
		WriteOrder::Create,
		WriteOrder::Update,
		WriteOrder::Delete,
		WriteOrder::UpsertCreate,
		WriteOrder::UpsertUpdate,
		WriteOrder::RelateCreate,
		WriteOrder::RelateUpdate,
		WriteOrder::InsertCreate,
		WriteOrder::InsertUpdate,
	];

	/// The form this order belongs to, and `None` for a view-maintenance
	/// order: those belong to no statement kind — a write reaches one by
	/// synthesising a row, not by being a `CREATE` or a `DELETE`.
	pub(crate) fn form(self) -> Option<WriteForm> {
		match self {
			WriteOrder::Create => Some(WriteForm::Create),
			WriteOrder::Update => Some(WriteForm::Update),
			WriteOrder::Delete => Some(WriteForm::Delete),
			WriteOrder::UpsertCreate | WriteOrder::UpsertUpdate => Some(WriteForm::Upsert),
			WriteOrder::RelateCreate | WriteOrder::RelateUpdate => Some(WriteForm::Relate),
			WriteOrder::InsertCreate | WriteOrder::InsertUpdate => Some(WriteForm::Insert),
			WriteOrder::ViewCreate | WriteOrder::ViewUpdate | WriteOrder::ViewDelete => None,
		}
	}

	/// The view-maintenance order for one action.
	pub(crate) fn for_view(action: Action) -> Self {
		match action {
			Action::Create => WriteOrder::ViewCreate,
			Action::Update => WriteOrder::ViewUpdate,
			Action::Delete => WriteOrder::ViewDelete,
		}
	}

	/// The order's name, as `EXPLAIN` labels its stage list.
	pub(crate) fn name(self) -> &'static str {
		match self {
			WriteOrder::Create => "create",
			WriteOrder::Update => "update",
			WriteOrder::Delete => "delete",
			WriteOrder::UpsertCreate => "upsert_create",
			WriteOrder::UpsertUpdate => "upsert_update",
			WriteOrder::RelateCreate => "relate_create",
			WriteOrder::RelateUpdate => "relate_update",
			WriteOrder::InsertCreate => "insert_create",
			WriteOrder::InsertUpdate => "insert_update",
			WriteOrder::ViewCreate => "view_create",
			WriteOrder::ViewUpdate => "view_update",
			WriteOrder::ViewDelete => "view_delete",
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
	/// `INSERT IGNORE`: a create failure this form does not retry is skipped
	/// rather than raised.
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
	/// pushed. This is the one place a stage's position, rather than its
	/// presence, carries a form's difference: `UPSERT` pushes after the tenant
	/// guard alone, `INSERT` after the tenant guard and the id derivation,
	/// because a rolled-back attempt must not derive a second id.
	pub(crate) savepoint_after: usize,
	/// Whether a failure naming an existing record may be re-run as an update.
	/// False for `INSERT` without `ON DUPLICATE KEY UPDATE`, which has no
	/// updating clause to run.
	pub(crate) retry_conflict: bool,
	/// Whether a create failure this policy does not retry is skipped rather
	/// than raised. True for `INSERT IGNORE`, and for it alone: the row is
	/// skipped whichever layer raised the failure, so a failure that moves
	/// layers does not silently stop being ignored.
	pub(crate) skip_failures: bool,
	/// Whether a schema failure re-runs as an update carrying the original
	/// failure, which is re-raised if the update fails too.
	pub(crate) carry_schema_failure: bool,
}

/// A form's orders and its branching, as data.
#[derive(Clone, Debug)]
pub(crate) struct WritePlan {
	form: WriteForm,
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
						// The tenant guard and the id derivation run outside
						// the savepoint: a rolled-back attempt must not derive
						// a second id.
						savepoint_after: 2,
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
			form: facts.form,
			orders,
			branching,
		}
	}

	/// The form these orders belong to.
	pub(crate) fn form(&self) -> WriteForm {
		self.form
	}

	/// How the orders are selected between.
	pub(crate) fn branching(&self) -> Branching {
		self.branching
	}

	/// The named order, when this form has one.
	pub(crate) fn order(&self, which: WriteOrder) -> Option<&[StageId]> {
		self.orders.iter().find(|(o, _)| *o == which).map(|(_, s)| s.as_slice())
	}

	/// Every order this form runs, in the order they are declared.
	pub(crate) fn orders(&self) -> impl Iterator<Item = (WriteOrder, &[StageId])> {
		self.orders.iter().map(|(o, s)| (*o, s.as_slice()))
	}

	/// The maintenance a view row gets for `action`, as a program of its own:
	/// the order [`WriteForm::View`]'s plan holds for the action, bound to its
	/// stages. The action is settled by the caller, which has just written or
	/// deleted the row and knows which it did.
	pub(crate) fn view_maintenance(action: Action) -> Result<WriteProgram, StageId> {
		let which = WriteOrder::for_view(action);
		let mut stages = Vec::new();
		for id in view_maintenance_order(action) {
			stages.push(stage_impl(id).ok_or(id)?);
		}
		Ok(WriteProgram {
			orders: vec![(which, stages)],
			branching: Branching::Sole(which),
		})
	}

	/// Bind each named stage to its implementation.
	///
	/// `Err` names the first stage whose implementation has not landed, which
	/// is the whole of the gap: the orders are complete, and a form becomes
	/// runnable exactly when every stage its orders name is implemented.
	pub(crate) fn resolve(&self) -> Result<WriteProgram, StageId> {
		let mut orders = Vec::with_capacity(self.orders.len());
		for (which, ids) in &self.orders {
			let mut stages = Vec::with_capacity(ids.len());
			for id in ids {
				stages.push(stage_impl(*id).ok_or(*id)?);
			}
			orders.push((*which, stages));
		}
		Ok(WriteProgram {
			orders,
			branching: self.branching,
		})
	}
}

/// The implementation of a stage, when one is registered.
///
/// An order names stages; [`super::stages`] is where a name becomes a body,
/// and this is the only call into it. A `None` has no implementation, and
/// [`WritePlan::resolve`] refuses to bind a plan that names it rather than
/// running a partial order — which is what keeps a half-built order from
/// writing anything.
///
/// # A stage reaches the statement's clauses through the context, not the id
///
/// `StageId` is `Copy`, so it can carry no `Arc<dyn PhysicalExpr>` and the
/// lookup is a pure function of a name. The statement's compiled `WHERE`, data
/// clause and `ON DUPLICATE KEY UPDATE` reach a stage through
/// [`WriteCtx::clauses`](super::ctx::WriteCtx), resolved once per statement and
/// shared by every record. A stage that wants one reads it from the context it
/// is handed; it is never a constructor argument, because one implementation of
/// a stage is shared by every statement of that form.
fn stage_impl(id: StageId) -> Option<Arc<dyn WriteStage>> {
	super::stages::lookup(id)
}

/// A form's orders with every stage bound, plus the control flow that selects
/// between them.
#[derive(Debug)]
pub(crate) struct WriteProgram {
	orders: Vec<(WriteOrder, Vec<Arc<dyn WriteStage>>)>,
	branching: Branching,
}

impl WriteProgram {
	/// Run one record.
	pub(crate) async fn run_record(
		&self,
		row: &mut WriteRow,
		ctx: &WriteCtx,
	) -> Result<StageOutcome, ControlFlow> {
		match self.branching {
			Branching::Sole(order) => self.run_order(order, row, ctx).await,
			Branching::UpFront {
				creating,
				updating,
			} => {
				// The record decides, before any order-specific stage runs, so
				// a subject that named an id is read here rather than by a
				// stage of the order this reading picks.
				row.load_named_subject(ctx).await?;
				let order = if row.is_new() {
					creating
				} else {
					updating
				};
				self.run_order(order, row, ctx).await
			}
			Branching::Retry {
				creating,
				updating,
				policy,
			} => {
				// A record the source already read has nothing to create.
				if !row.is_iteration_initial() {
					return self.run_order(updating, row, ctx).await;
				}
				self.attempt_create(creating, updating, policy, row, ctx).await
			}
			Branching::ByAction => {
				Err(ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InvalidStatement(
					"a view row's order is chosen by the action its maintenance settled"
						.to_string()
				))))
			}
		}
	}

	/// Attempt the creating order behind a savepoint, and re-run a classified
	/// conflict as an update.
	async fn attempt_create(
		&self,
		creating: WriteOrder,
		updating: WriteOrder,
		policy: RetryPolicy,
		row: &mut WriteRow,
		ctx: &WriteCtx,
	) -> Result<StageOutcome, ControlFlow> {
		let stages = self.stages(creating)?;
		let split = policy.savepoint_after.min(stages.len());
		// Stages ahead of the savepoint are not part of the attempt and are
		// not undone by rolling it back.
		match run_stages(&stages[..split], row, ctx).await? {
			StageOutcome::SkipRow => return Ok(StageOutcome::SkipRow),
			StageOutcome::Continue => {}
		}
		let txn = ctx.txn();
		txn.new_save_point().await.map_err(ControlFlow::Err)?;
		let outcome = run_stages(&stages[split..], row, ctx).await;
		let failure = match outcome {
			Ok(outcome) => {
				txn.release_last_save_point().await.map_err(ControlFlow::Err)?;
				return Ok(outcome);
			}
			// `BREAK`, `CONTINUE` and `RETURN` belong to a construct outside
			// this statement and are not create conflicts, so they are not
			// reclassified — but the attempt behind them is half-run, and the
			// savepoint must be rolled back before the signal leaves. Releasing
			// it would commit a record stored without its index entries, its
			// edges or its observers into the enclosing transaction.
			//
			// A stage that propagates one is rolled back like any other
			// failure, so the signal never leaves a half-written record behind.
			Err(signal @ (ControlFlow::Break | ControlFlow::Continue | ControlFlow::Return(_))) => {
				txn.rollback_to_save_point().await.map_err(ControlFlow::Err)?;
				row.mutated = false;
				return Err(signal);
			}
			Err(ControlFlow::Err(err)) => classify_create_failure(err, row, policy),
		};
		// Undo the attempt before anything else looks at the transaction, and
		// clear the record's mutation flag so a half-written record is not
		// counted as affected.
		txn.rollback_to_save_point().await.map_err(ControlFlow::Err)?;
		row.mutated = false;
		let (target, carried) = match failure {
			CreateFailure::Skip => return Ok(StageOutcome::SkipRow),
			CreateFailure::Raise(err) => return Err(ControlFlow::Err(err)),
			CreateFailure::RetryAsUpdate {
				record,
				carried,
			} => (record, carried),
		};
		// A statement that ran out of time between the attempt and the retry
		// abandons the record rather than starting a second pass on it.
		if ctx.exec().ctx().is_done(None).await.map_err(ControlFlow::Err)? {
			return Ok(StageOutcome::SkipRow);
		}
		let stored = ctx
			.txn()
			.get_record(
				ctx.ns.namespace_id,
				ctx.db.database_id,
				&target.table,
				&target.key,
				ctx.exec().version_stamp(),
			)
			.await
			.map_err(ControlFlow::Err)?;
		row.reset_for_update_retry(target, &stored);
		let outcome = self.run_order(updating, row, ctx).await;
		// A schema failure the attempt carried over is the caller's answer
		// unless the update produced a record, which means the schema was
		// satisfied by the stored one. A skipped record is not a success: the
		// record the statement was asked to write still does not conform.
		match (carried, outcome) {
			(Some(err), Err(_) | Ok(StageOutcome::SkipRow)) => Err(ControlFlow::Err(err)),
			(_, outcome) => outcome,
		}
	}

	/// Run every stage of one order.
	async fn run_order(
		&self,
		which: WriteOrder,
		row: &mut WriteRow,
		ctx: &WriteCtx,
	) -> Result<StageOutcome, ControlFlow> {
		let stages = self.stages(which)?;
		run_stages(stages, row, ctx).await
	}

	/// The bound stages of one order.
	fn stages(&self, which: WriteOrder) -> Result<&[Arc<dyn WriteStage>], ControlFlow> {
		self.orders.iter().find(|(o, _)| *o == which).map(|(_, s)| s.as_slice()).ok_or_else(|| {
			ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InvalidStatement(format!(
				"the write plan holds no {} order",
				which.name()
			))))
		})
	}
}

/// Apply a run of stages to one record, stopping at the first that skips it.
async fn run_stages(
	stages: &[Arc<dyn WriteStage>],
	row: &mut WriteRow,
	ctx: &WriteCtx,
) -> Result<StageOutcome, ControlFlow> {
	for stage in stages {
		match stage.apply(row, ctx).await? {
			StageOutcome::Continue => {}
			StageOutcome::SkipRow => return Ok(StageOutcome::SkipRow),
		}
	}
	Ok(StageOutcome::Continue)
}

/// What a rolled-back creating attempt becomes.
pub(crate) enum CreateFailure {
	/// Run the updating order against this record, carrying the original
	/// failure when it is one the update is expected to resolve.
	RetryAsUpdate {
		record: RecordId,
		carried: Option<anyhow::Error>,
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
		};
	}
	// A schema failure is retried against the record the attempt was building,
	// because the stored record the update reads may satisfy the clause the
	// attempt's partial record did not.
	if policy.carry_schema_failure
		&& err.downcast_ref::<DocError>().is_some_and(DocError::is_schema_related)
		&& let Some(id) = row.id.clone()
	{
		return CreateFailure::RetryAsUpdate {
			record: id,
			carried: Some(err),
		};
	}
	unretried(err, policy)
}

/// A create failure this policy does not retry: skipped whichever layer raised
/// it under `INSERT IGNORE`, and raised otherwise.
fn unretried(err: anyhow::Error, policy: RetryPolicy) -> CreateFailure {
	if policy.skip_failures {
		CreateFailure::Skip
	} else {
		CreateFailure::Raise(err)
	}
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

/// The `INSERT` create order, whose id derivation sits on the dispatch
/// ahead of the savepoint rather than inside `insert_create`. It is the only
/// creating order that applies its data by merging the row's own value, and the
/// only creating order that validates the special fields before applying the
/// data; the four updating orders all do, so the split is creating-vs-updating
/// with this one order on the other side.
fn insert_create_order() -> Vec<StageId> {
	vec![
		StageId::CheckRecordUserAccess,
		StageId::GenerateRecordId,
		StageId::CheckTableType(TableTypeFor::Insert),
		StageId::CheckPermissionsQuickCreate,
		StageId::CheckTableNotView,
		StageId::ComputeInputData,
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

	/// Facts for a form with a data clause and none of the `INSERT` modifiers,
	/// which is the shape every order below is transcribed at.
	fn facts(form: WriteForm) -> WriteFacts {
		WriteFacts {
			form,
			has_data: true,
			on_duplicate_key_update: false,
			ignore: false,
			repeatable: false,
		}
	}

	fn order_of(form: WriteForm, which: WriteOrder) -> Vec<StageId> {
		WritePlan::build(facts(form))
			.order(which)
			.unwrap_or_else(|| panic!("{} has no {} order", form.kind(), which.name()))
			.to_vec()
	}

	/// The `CREATE` order's stages, in order.
	#[test]
	fn create_order_matches_the_document_pipeline() {
		assert_eq!(
			order_of(WriteForm::Create, WriteOrder::Create),
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
		);
	}

	/// The `UPDATE` order's stages, in order.
	#[test]
	fn update_order_matches_the_document_pipeline() {
		assert_eq!(
			order_of(WriteForm::Update, WriteOrder::Update),
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
		);
	}

	/// The `DELETE` order's stages, in order.
	#[test]
	fn delete_order_matches_the_document_pipeline() {
		assert_eq!(
			order_of(WriteForm::Delete, WriteOrder::Delete),
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
		);
	}

	/// The `UPSERT` create order's stages, in order.
	#[test]
	fn upsert_create_order_matches_the_document_pipeline() {
		assert_eq!(
			order_of(WriteForm::Upsert, WriteOrder::UpsertCreate),
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
		);
	}

	/// The `UPSERT` update order's stages, in order.
	#[test]
	fn upsert_update_order_matches_the_document_pipeline() {
		assert_eq!(
			order_of(WriteForm::Upsert, WriteOrder::UpsertUpdate),
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
		);
	}

	/// The `RELATE` create order's stages, in order, with a data clause.
	#[test]
	fn relate_create_order_matches_the_document_pipeline() {
		assert_eq!(
			order_of(WriteForm::Relate, WriteOrder::RelateCreate),
			vec![
				StageId::CheckRecordUserAccess,
				StageId::CheckPermissionsQuickCreate,
				StageId::CheckTableNotView,
				StageId::ComputeInputData,
				StageId::ProcessRecordData,
				StageId::GenerateRecordId,
				StageId::CheckTableType(TableTypeFor::Relate),
				StageId::CheckTableNotLightweight(LightweightReason::GivenADataClause),
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
			]
		);
	}

	/// The `RELATE` update order's stages, in order, with a data clause.
	/// The one updating order with no inline adjacency.
	#[test]
	fn relate_update_order_matches_the_document_pipeline() {
		assert_eq!(
			order_of(WriteForm::Relate, WriteOrder::RelateUpdate),
			vec![
				StageId::CheckRecordUserAccess,
				StageId::CheckRecordExists,
				StageId::CheckTableType(TableTypeFor::Relate),
				StageId::CheckUpdatePermissions,
				StageId::CheckTableNotView,
				StageId::CheckTableNotLightweight(LightweightReason::GivenADataClause),
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
			]
		);
	}

	/// The `INSERT` create order's stages, in order, with the id derivation
	/// the dispatch performs ahead of the savepoint.
	#[test]
	fn insert_create_order_matches_the_document_pipeline() {
		assert_eq!(
			order_of(WriteForm::Insert, WriteOrder::InsertCreate),
			vec![
				StageId::CheckRecordUserAccess,
				StageId::GenerateRecordId,
				StageId::CheckTableType(TableTypeFor::Insert),
				StageId::CheckPermissionsQuickCreate,
				StageId::CheckTableNotView,
				StageId::ComputeInputData,
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
		);
	}

	/// The `INSERT` update order's stages, in order.
	#[test]
	fn insert_update_order_matches_the_document_pipeline() {
		assert_eq!(
			order_of(WriteForm::Insert, WriteOrder::InsertUpdate),
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
		);
	}

	/// Every form declares exactly the orders its branching selects between,
	/// and the nine orders across the six forms are the nine this module names.
	#[test]
	fn the_six_forms_declare_the_nine_orders() {
		let mut declared = Vec::new();
		for form in [
			WriteForm::Create,
			WriteForm::Update,
			WriteForm::Upsert,
			WriteForm::Delete,
			WriteForm::Relate,
			WriteForm::Insert,
		] {
			let plan = WritePlan::build(facts(form));
			for (which, _) in plan.orders() {
				assert_eq!(
					which.form(),
					Some(form),
					"{} declared a {} order",
					form.kind(),
					which.name()
				);
				declared.push(which);
			}
			match plan.branching() {
				Branching::Sole(order) => {
					assert!(plan.order(order).is_some());
					assert_eq!(plan.orders().count(), 1);
				}
				Branching::UpFront {
					creating,
					updating,
				} => {
					assert!(plan.order(creating).is_some());
					assert!(plan.order(updating).is_some());
					assert_eq!(plan.orders().count(), 2);
				}
				Branching::Retry {
					creating,
					updating,
					policy,
				} => {
					let create = plan.order(creating).expect("a retrying form has a create order");
					assert!(plan.order(updating).is_some());
					assert_eq!(plan.orders().count(), 2);
					assert!(policy.savepoint_after <= create.len());
				}
				Branching::ByAction => panic!("{} chose its order by action", form.kind()),
			}
		}
		assert_eq!(declared.len(), WriteOrder::ALL.len());
		for order in WriteOrder::ALL {
			assert!(declared.contains(&order), "{} is declared by no form", order.name());
		}
	}

	/// Tenant confinement heads all nine orders. The read path enforces it at
	/// the scan operators; a write performs no scan, so an order that omits it
	/// reintroduces a fixed vulnerability.
	#[test]
	fn tenant_confinement_heads_every_order() {
		for form in [
			WriteForm::Create,
			WriteForm::Update,
			WriteForm::Upsert,
			WriteForm::Delete,
			WriteForm::Relate,
			WriteForm::Insert,
		] {
			let plan = WritePlan::build(facts(form));
			for (which, stages) in plan.orders() {
				assert_eq!(
					stages.first().map(|s| s.name()),
					Some("check_record_user_access"),
					"{} does not open with the tenant guard",
					which.name()
				);
			}
		}
	}

	/// The savepoint sits after the stages that must survive a rolled-back
	/// attempt: the tenant guard on `UPSERT`, and the tenant guard plus the id
	/// derivation on `INSERT`, which must not derive a second id.
	#[test]
	fn the_savepoint_sits_where_the_dispatch_pushes_it() {
		let upsert = WritePlan::build(facts(WriteForm::Upsert));
		let Branching::Retry {
			creating,
			policy,
			..
		} = upsert.branching()
		else {
			panic!("UPSERT retries");
		};
		let order = upsert.order(creating).expect("UPSERT has a create order");
		assert_eq!(policy.savepoint_after, 1);
		assert_eq!(order[0], StageId::CheckRecordUserAccess);

		let insert = WritePlan::build(facts(WriteForm::Insert));
		let Branching::Retry {
			creating,
			policy,
			..
		} = insert.branching()
		else {
			panic!("INSERT retries");
		};
		let order = insert.order(creating).expect("INSERT has a create order");
		assert_eq!(policy.savepoint_after, 2);
		assert_eq!(order[0], StageId::CheckRecordUserAccess);
		assert_eq!(order[1], StageId::GenerateRecordId);
	}

	/// `RELATE` guards the lightweight edge only when the statement carries a
	/// data clause, and both of its orders agree about that.
	#[test]
	fn relate_guards_a_lightweight_edge_only_with_data() {
		let guard = StageId::CheckTableNotLightweight(LightweightReason::GivenADataClause);
		let with_data = WritePlan::build(facts(WriteForm::Relate));
		for (_, stages) in with_data.orders() {
			assert!(stages.contains(&guard));
		}
		let without_data = WritePlan::build(WriteFacts {
			has_data: false,
			..facts(WriteForm::Relate)
		});
		for (_, stages) in without_data.orders() {
			assert!(!stages.contains(&guard));
		}
	}

	/// `INSERT`'s retry policy is the statement's clauses, not the form's:
	/// a conflict is retried only with `ON DUPLICATE KEY UPDATE`, and a failure
	/// that is not retried is skipped only under `IGNORE`.
	#[test]
	fn insert_retry_policy_follows_the_statement() {
		let plain = WritePlan::build(facts(WriteForm::Insert));
		let Branching::Retry {
			policy,
			..
		} = plain.branching()
		else {
			panic!("INSERT retries");
		};
		assert!(!policy.retry_conflict);
		assert!(!policy.skip_failures);

		let modified = WritePlan::build(WriteFacts {
			on_duplicate_key_update: true,
			ignore: true,
			..facts(WriteForm::Insert)
		});
		let Branching::Retry {
			policy,
			..
		} = modified.branching()
		else {
			panic!("INSERT retries");
		};
		assert!(policy.retry_conflict);
		assert!(policy.skip_failures);
	}

	/// Every stage this module names is placed in at least one order. A stage
	/// that lands with nowhere to run is a stage whose order was never decided.
	#[test]
	fn every_stage_is_used_by_some_order() {
		let mut used: Vec<StageId> = Vec::new();
		for form in [
			WriteForm::Create,
			WriteForm::Update,
			WriteForm::Upsert,
			WriteForm::Delete,
			WriteForm::Relate,
			WriteForm::Insert,
		] {
			for has_data in [true, false] {
				let plan = WritePlan::build(WriteFacts {
					has_data,
					..facts(form)
				});
				for (_, stages) in plan.orders() {
					for stage in stages {
						if !used.contains(stage) {
							used.push(*stage);
						}
					}
				}
			}
		}
		for expected in StageId::ALL {
			assert!(used.contains(expected), "{expected:?} is placed in no order");
		}
	}

	/// Every stage has a body, so every form resolves and none is declined for
	/// a stage it names.
	#[test]
	fn every_stage_has_a_body() {
		for id in StageId::ALL {
			assert!(stage_impl(*id).is_some(), "{id:?} has no body");
		}
	}

	/// Resolving a form either binds every stage its orders name or binds none
	/// of them, and names the first stage with no body rather than running a
	/// partial order.
	///
	/// The expectation is derived from the registry rather than written down,
	/// so the test states the contract for as long as stages keep landing
	/// instead of pinning which stage is missing on the day it was written.
	#[test]
	fn resolving_reports_the_first_unimplemented_stage() {
		for form in [
			WriteForm::Create,
			WriteForm::Update,
			WriteForm::Upsert,
			WriteForm::Delete,
			WriteForm::Relate,
			WriteForm::Insert,
		] {
			let plan = WritePlan::build(facts(form));
			// The orders in the sequence `resolve` walks them, so the first
			// unbound stage here is the one it must report.
			let unbound = plan
				.orders()
				.flat_map(|(_, stages)| stages.iter().copied())
				.find(|id| stage_impl(*id).is_none());
			match (plan.resolve(), unbound) {
				(Ok(_), None) => {}
				(Err(reported), Some(expected)) => assert_eq!(
					reported, expected,
					"{form:?} reported {reported:?} rather than its first unbound stage"
				),
				(Ok(_), Some(expected)) => {
					panic!("{form:?} bound an order although {expected:?} has no body")
				}
				(Err(reported), None) => {
					panic!(
						"{form:?} reported {reported:?} although every stage it names has a body"
					)
				}
			}
		}
	}
}
