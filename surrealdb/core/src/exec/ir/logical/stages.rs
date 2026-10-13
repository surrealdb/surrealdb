//! Write-stage regions and the logical alternatives a statement offers.
//!
//! A write statement loops over its targets. The loop body is a sequence of write stages, run in
//! the order the executor performs them for one record. Each stage is an op that carries its kind
//! and its mark, so an effect or a failure raised in a stage is attributed to that stage, and a
//! dropped stage is dropped whole. A statement with more than one equivalent plan holds them as the
//! arms of a logical `choose`. The generic arm is the one every input admits.
//!
//! The orders come from the executor. [`orders`] reads them from the write plan the executor
//! builds, so the logical order and the executed order always match. A stage's mark depends only on
//! the stage's code, never on the statement that runs it.
//!
//! A statement binds one stage op for each stage in [`stage_run`]. The permission clause a table
//! stores for a gate stage is stated by the write ([`WriteGates`], from [`gates_of`]). A clause
//! that is a predicate is the `PermissionFrame` region of that stage op.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;
use surrealdb_strand::{Strand, TableName};
use surrealdb_types::ToSql as _;

use super::ops::{
	DerivedField, EventClause, EventDefs, EventShape, EventsAttrs, FieldClause, FieldClauses,
	FieldGate, FieldStep, GateClause, ImageLayers, ImageView, IndexDefs, LogOp, LoopKind,
	RepairField, RepairKind, RepairTable, Stored, TableDefs, WriteBody, WriteBranching,
	WriteDerived, WriteGates, WriteImage, WriteIndexes, WriteMark, WriteRepairs, WriteRetry,
	WriteSchema, WriteStageKind, WriteStored, WriteViews,
};
use super::seam::invariant;
use crate::catalog::aggregation::AggregateFields;
use crate::catalog::{
	DefineDefault, EventDefinition, FieldDefinition, IndexDefinition, KeyOrigin, KeyStructure,
	Permission, PermissionKind, TableDefinition, ViewDefinition,
};
use crate::doc::Action;
use crate::exec::catalog_deps::Referencing;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::catalog::{CatalogAnswer, SemanticCatalog};
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::iface::region_kind::{ClauseRole, FieldRole, RegionKind};
use crate::exec::ir::iface::statements::WriteForm;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::operators::scan::pipeline::narrowing_auth_limit;
use crate::exec::operators::write::plan::{
	Branching, WriteFacts, WriteForm as PlanForm, WriteOrder, WritePlan,
};
use crate::exec::operators::write::stage::StageId;
use crate::expr::logical::{
	EvalFrame, GuardKind, LogicalExpr, LogicalFields, LogicalOp, LogicalPermission, LogicalWrite,
	LogicalWriteSource, LogicalWriteStatement,
};
use crate::expr::reference::ReferenceDeleteStrategy;
use crate::expr::{Expr, Part};

/// The one table `write` writes to, when the statement names it. A table or record id target counts
/// (a `RELATE`'s target is its `through`), and so does an `INSERT`'s `INTO`. With no `INTO`, the
/// table comes from the record ids of the literal rows, or from the table or records that a read
/// yields. A table or record is named by a literal, a mock, or a `type::table` or two-argument
/// `type::record` call whose table is a non-empty string literal. Such a call yields a value of
/// that table or fails. Returns `None` when the targets are otherwise evaluated, when they name
/// more than one table, or when they come from anything else. A subquery among them yields records
/// of whatever tables its values name, not of the table it reads.
pub(crate) fn written_table(write: &LogicalWrite) -> Option<TableName> {
	use crate::expr::Function;
	use crate::expr::logical::{
		LogicalInsertRows, LogicalLiteral, LogicalSource, LogicalValues, SourceRef,
	};
	use crate::expr::mock::Mock;
	fn mock_table(mock: &Mock) -> TableName {
		match mock {
			Mock::Count(table, _) | Mock::Range(table, _) => table.clone(),
		}
	}
	#[allow(clippy::wildcard_enum_match_arm)]
	fn named(expr: &LogicalExpr) -> Option<TableName> {
		match expr {
			LogicalExpr::Table(table) => Some(table.clone()),
			LogicalExpr::RecordId(rid) => Some(rid.table.clone()),
			LogicalExpr::Mock(mock) => Some(mock_table(mock)),
			LogicalExpr::Call(call) => {
				let Function::Normal(name) = &call.receiver else {
					return None;
				};
				let table = match call.args.first() {
					Some(LogicalExpr::Literal(LogicalLiteral::String(table)))
						if !table.is_empty() =>
					{
						TableName::new(table.as_str())
					}
					_ => return None,
				};
				match (name.as_str(), call.args.len()) {
					("type::table", 1) | ("type::record", 2) => Some(table),
					_ => None,
				}
			}
			_ => None,
		}
	}
	/// Pushes the table `expr`, or each element of it where it is a literal list, names.
	#[allow(clippy::wildcard_enum_match_arm)]
	fn each_named(expr: &LogicalExpr, tables: &mut Vec<TableName>) -> bool {
		let mut push = |expr: &LogicalExpr| match named(expr) {
			Some(table) => {
				tables.push(table);
				true
			}
			None => false,
		};
		match expr {
			LogicalExpr::Array(items) => items.iter().all(push),
			other => push(other),
		}
	}
	/// Pushes the table of the record id literal each literal row of `rows` is written with.
	#[allow(clippy::wildcard_enum_match_arm)]
	fn row_ids(rows: &LogicalExpr, tables: &mut Vec<TableName>) -> bool {
		let mut row = |row: &LogicalExpr| {
			let LogicalExpr::Object(fields) = row else {
				return false;
			};
			match fields.iter().find(|(field, _)| field == "id") {
				Some((_, LogicalExpr::RecordId(rid))) => {
					tables.push(rid.table.clone());
					true
				}
				_ => false,
			}
		};
		match rows {
			LogicalExpr::Array(items) => items.iter().all(row),
			other => row(other),
		}
	}
	#[allow(clippy::wildcard_enum_match_arm)]
	fn read(op: &LogicalOp, tables: &mut Vec<TableName>) -> bool {
		match op {
			LogicalOp::Source(LogicalSource {
				source,
				..
			}) => match source {
				SourceRef::Table(table) => tables.push(table.clone()),
				SourceRef::RecordId(rid) => tables.push(rid.table.clone()),
				SourceRef::Mock(mock) => tables.push(mock_table(mock)),
				SourceRef::Dynamic(expr) => match named(expr) {
					Some(table) => tables.push(table),
					None => return false,
				},
				SourceRef::Param(_) => return false,
			},
			LogicalOp::Values(LogicalValues {
				expr,
				..
			}) if !each_named(expr, tables) => return false,
			LogicalOp::Subquery(_) => return false,
			_ => {}
		}
		op.children().iter().all(|child| read(child, tables))
	}
	let mut tables: Vec<TableName> = Vec::new();
	let known = match &write.source {
		LogicalWriteSource::Targets(targets) => {
			let targets: Vec<&LogicalExpr> =
				if matches!(write.statement, LogicalWriteStatement::Relate(_)) {
					targets.get(1).into_iter().collect()
				} else {
					targets.iter().collect()
				};
			targets.into_iter().all(|target| match named(target) {
				Some(table) => {
					tables.push(table);
					true
				}
				None => false,
			})
		}
		LogicalWriteSource::Inserts(inserts) => match (&inserts.into, &inserts.rows) {
			(Some(into), _) => match named(into) {
				Some(table) => {
					tables.push(table);
					true
				}
				None => false,
			},
			(None, LogicalInsertRows::Value(rows)) => row_ids(rows, &mut tables),
			(None, LogicalInsertRows::Tuples(_)) => false,
		},
		LogicalWriteSource::Rows(input) => read(input, &mut tables),
	};
	let first = tables.first()?.clone();
	(known && tables.iter().all(|table| *table == first)).then_some(first)
}

/// The text of the statement `write` performs, as written: the form a write that names no one
/// table prints its statement in ([`statement_of_text`] reads it back).
pub(crate) fn statement_text(write: &LogicalWrite) -> String {
	match &write.statement {
		LogicalWriteStatement::Create(statement) => statement.to_sql(),
		LogicalWriteStatement::Update(statement) => statement.to_sql(),
		LogicalWriteStatement::Upsert(statement) => statement.to_sql(),
		LogicalWriteStatement::Delete(statement) => statement.to_sql(),
		LogicalWriteStatement::Insert(statement) => statement.to_sql(),
		LogicalWriteStatement::Relate(statement) => statement.to_sql(),
	}
}

/// The write the statement text `text` lowers to, before any planning rewrites it, or why it
/// names no write.
pub(crate) fn statement_of_text(text: &str) -> Result<LogicalWrite, String> {
	let parsed = crate::syn::parse(text).map_err(|e| format!("`{text}` does not parse: {e}"))?;
	let mut statements = parsed.expressions.into_iter();
	let (Some(surrealdb_expr::parser::TopLevelExpr::Expr(expr)), None) =
		(statements.next(), statements.next())
	else {
		return Err(format!("`{text}` is not one statement"));
	};
	match crate::expr::logical::lower(expr) {
		Ok(LogicalOp::Write(write)) => Ok(write),
		Ok(_) => Err(format!("`{text}` is not a write")),
		Err(e) => Err(format!("`{text}` does not lower: {e:?}")),
	}
}

/// The `INTO` expression of an `INSERT` as the statement wrote it, which a refusal of a value that
/// names no table quotes. `None` for a statement that has no `INTO`.
pub(crate) fn into_text_of(write: &LogicalWrite) -> Option<Box<str>> {
	match &write.source {
		LogicalWriteSource::Inserts(inserts) => {
			inserts.into.as_ref().map(|into| into.to_sql().into_boxed_str())
		}
		LogicalWriteSource::Targets(_) | LogicalWriteSource::Rows(_) => None,
	}
}

/// The half of the work a stage does, decided by the stage's code alone.
///
/// A `Compute` stage reads the row's images and the catalog, and raises errors. It never writes
/// shared state. Every other stage writes shared state, queues work, sends notifications, or
/// projects the stored result, and is `Apply`. In every order of the write plan, all `Compute`
/// stages come before all `Apply` stages, so an order splits at its marks at most once. A record id
/// drawn at random is entropy, and the stage's placement decides it. The static mark of
/// `GenerateRecordId` stays `Compute`.
pub(crate) fn static_mark(stage: WriteStageKind) -> WriteMark {
	match stage {
		WriteStageKind::CheckRecordUserAccess
		| WriteStageKind::CheckRecordExists
		| WriteStageKind::CheckTableNotView
		| WriteStageKind::CheckTableNotLightweight(_)
		| WriteStageKind::CheckTableType(_)
		| WriteStageKind::CheckPermissionsQuickCreate
		| WriteStageKind::CheckUpdatePermissions
		| WriteStageKind::CheckDeletePermissions
		| WriteStageKind::CheckCreatePermissions
		| WriteStageKind::RecheckUpdatePermissions
		| WriteStageKind::CheckWhereCondition
		| WriteStageKind::ComputeInputData
		| WriteStageKind::CheckDataFields
		| WriteStageKind::ProcessRecordData
		| WriteStageKind::ProcessMergeData
		| WriteStageKind::DefaultRecordData
		| WriteStageKind::GenerateRecordId
		| WriteStageKind::ProcessTableFields
		| WriteStageKind::CleanupTableFields => WriteMark::Compute,
		WriteStageKind::CleanupTableReferences
		| WriteStageKind::ClearRecordData
		| WriteStageKind::StoreRecordData
		| WriteStageKind::StoreEdgesData
		| WriteStageKind::StoreInlineAdjacencyData
		| WriteStageKind::StoreIndexData
		| WriteStageKind::ReclaimDocId
		| WriteStageKind::PurgeRecordData
		| WriteStageKind::MaterialiseObservedFields
		| WriteStageKind::ProcessTableReferences
		| WriteStageKind::ProcessTableViews(_)
		| WriteStageKind::ProcessTableEvents(_)
		| WriteStageKind::ProcessTableLives(_)
		| WriteStageKind::ProcessChangefeeds
		| WriteStageKind::CheckSelectPermissions(_)
		| WriteStageKind::OutputWrite => WriteMark::Apply,
	}
}

/// The IR's name for an executor stage. The two enums name the same stages with the same
/// payloads, so the map is total and [`stage_id`] is its inverse.
pub(crate) fn stage_kind(id: StageId) -> WriteStageKind {
	match id {
		StageId::CheckRecordUserAccess => WriteStageKind::CheckRecordUserAccess,
		StageId::CheckRecordExists => WriteStageKind::CheckRecordExists,
		StageId::CheckTableNotView => WriteStageKind::CheckTableNotView,
		StageId::CheckTableNotLightweight(reason) => {
			WriteStageKind::CheckTableNotLightweight(reason)
		}
		StageId::CheckTableType(form) => WriteStageKind::CheckTableType(form),
		StageId::CheckPermissionsQuickCreate => WriteStageKind::CheckPermissionsQuickCreate,
		StageId::CheckUpdatePermissions => WriteStageKind::CheckUpdatePermissions,
		StageId::CheckDeletePermissions => WriteStageKind::CheckDeletePermissions,
		StageId::CheckCreatePermissions => WriteStageKind::CheckCreatePermissions,
		StageId::RecheckUpdatePermissions => WriteStageKind::RecheckUpdatePermissions,
		StageId::CheckWhereCondition => WriteStageKind::CheckWhereCondition,
		StageId::ComputeInputData => WriteStageKind::ComputeInputData,
		StageId::CheckDataFields => WriteStageKind::CheckDataFields,
		StageId::ProcessRecordData => WriteStageKind::ProcessRecordData,
		StageId::ProcessMergeData => WriteStageKind::ProcessMergeData,
		StageId::DefaultRecordData => WriteStageKind::DefaultRecordData,
		StageId::GenerateRecordId => WriteStageKind::GenerateRecordId,
		StageId::ProcessTableFields => WriteStageKind::ProcessTableFields,
		StageId::CleanupTableFields => WriteStageKind::CleanupTableFields,
		StageId::CleanupTableReferences => WriteStageKind::CleanupTableReferences,
		StageId::ClearRecordData => WriteStageKind::ClearRecordData,
		StageId::StoreRecordData => WriteStageKind::StoreRecordData,
		StageId::StoreEdgesData => WriteStageKind::StoreEdgesData,
		StageId::StoreInlineAdjacencyData => WriteStageKind::StoreInlineAdjacencyData,
		StageId::StoreIndexData => WriteStageKind::StoreIndexData,
		StageId::ReclaimDocId => WriteStageKind::ReclaimDocId,
		StageId::PurgeRecordData => WriteStageKind::PurgeRecordData,
		StageId::MaterialiseObservedFields => WriteStageKind::MaterialiseObservedFields,
		StageId::ProcessTableReferences => WriteStageKind::ProcessTableReferences,
		StageId::ProcessTableViews(action) => WriteStageKind::ProcessTableViews(action),
		StageId::ProcessTableEvents(action) => WriteStageKind::ProcessTableEvents(action),
		StageId::ProcessTableLives(action) => WriteStageKind::ProcessTableLives(action),
		StageId::ProcessChangefeeds => WriteStageKind::ProcessChangefeeds,
		StageId::CheckSelectPermissions(image) => WriteStageKind::CheckSelectPermissions(image),
		StageId::OutputWrite => WriteStageKind::OutputWrite,
	}
}

/// The executor's name for an IR stage: the inverse of [`stage_kind`].
pub(crate) fn stage_id(kind: WriteStageKind) -> StageId {
	match kind {
		WriteStageKind::CheckRecordUserAccess => StageId::CheckRecordUserAccess,
		WriteStageKind::CheckRecordExists => StageId::CheckRecordExists,
		WriteStageKind::CheckTableNotView => StageId::CheckTableNotView,
		WriteStageKind::CheckTableNotLightweight(reason) => {
			StageId::CheckTableNotLightweight(reason)
		}
		WriteStageKind::CheckTableType(form) => StageId::CheckTableType(form),
		WriteStageKind::CheckPermissionsQuickCreate => StageId::CheckPermissionsQuickCreate,
		WriteStageKind::CheckUpdatePermissions => StageId::CheckUpdatePermissions,
		WriteStageKind::CheckDeletePermissions => StageId::CheckDeletePermissions,
		WriteStageKind::CheckCreatePermissions => StageId::CheckCreatePermissions,
		WriteStageKind::RecheckUpdatePermissions => StageId::RecheckUpdatePermissions,
		WriteStageKind::CheckWhereCondition => StageId::CheckWhereCondition,
		WriteStageKind::ComputeInputData => StageId::ComputeInputData,
		WriteStageKind::CheckDataFields => StageId::CheckDataFields,
		WriteStageKind::ProcessRecordData => StageId::ProcessRecordData,
		WriteStageKind::ProcessMergeData => StageId::ProcessMergeData,
		WriteStageKind::DefaultRecordData => StageId::DefaultRecordData,
		WriteStageKind::GenerateRecordId => StageId::GenerateRecordId,
		WriteStageKind::ProcessTableFields => StageId::ProcessTableFields,
		WriteStageKind::CleanupTableFields => StageId::CleanupTableFields,
		WriteStageKind::CleanupTableReferences => StageId::CleanupTableReferences,
		WriteStageKind::ClearRecordData => StageId::ClearRecordData,
		WriteStageKind::StoreRecordData => StageId::StoreRecordData,
		WriteStageKind::StoreEdgesData => StageId::StoreEdgesData,
		WriteStageKind::StoreInlineAdjacencyData => StageId::StoreInlineAdjacencyData,
		WriteStageKind::StoreIndexData => StageId::StoreIndexData,
		WriteStageKind::ReclaimDocId => StageId::ReclaimDocId,
		WriteStageKind::PurgeRecordData => StageId::PurgeRecordData,
		WriteStageKind::MaterialiseObservedFields => StageId::MaterialiseObservedFields,
		WriteStageKind::ProcessTableReferences => StageId::ProcessTableReferences,
		WriteStageKind::ProcessTableViews(action) => StageId::ProcessTableViews(action),
		WriteStageKind::ProcessTableEvents(action) => StageId::ProcessTableEvents(action),
		WriteStageKind::ProcessTableLives(action) => StageId::ProcessTableLives(action),
		WriteStageKind::ProcessChangefeeds => StageId::ProcessChangefeeds,
		WriteStageKind::CheckSelectPermissions(image) => StageId::CheckSelectPermissions(image),
		WriteStageKind::OutputWrite => StageId::OutputWrite,
	}
}

/// The executor's statement form for the IR's. The IR's form never names a view, which only view
/// maintenance writes.
pub(crate) fn plan_form(form: WriteForm) -> PlanForm {
	match form {
		WriteForm::Create => PlanForm::Create,
		WriteForm::Update => PlanForm::Update,
		WriteForm::Upsert => PlanForm::Upsert,
		WriteForm::Delete => PlanForm::Delete,
		WriteForm::Insert => PlanForm::Insert,
		WriteForm::Relate => PlanForm::Relate,
	}
}

/// The orders `form` selects between, each with the stages the write plan lists for it.
///
/// `facts` shape the orders; its own `form` is replaced by `form`, so the two cannot disagree.
/// A creating-or-updating form answers its creating order first.
pub(crate) fn orders(
	form: PlanForm,
	facts: WriteFacts,
) -> SmallVec<[(WriteOrder, Box<[WriteStageKind]>); 2]> {
	let plan = WritePlan::build(WriteFacts {
		form,
		..facts
	});
	let which: &[WriteOrder] = match form {
		PlanForm::Create => &[WriteOrder::Create],
		PlanForm::Update => &[WriteOrder::Update],
		PlanForm::Delete => &[WriteOrder::Delete],
		PlanForm::Upsert => &[WriteOrder::UpsertCreate, WriteOrder::UpsertUpdate],
		PlanForm::Relate => &[WriteOrder::RelateCreate, WriteOrder::RelateUpdate],
		PlanForm::Insert => &[WriteOrder::InsertCreate, WriteOrder::InsertUpdate],
		PlanForm::View => &[WriteOrder::ViewCreate, WriteOrder::ViewUpdate, WriteOrder::ViewDelete],
	};
	which
		.iter()
		.filter_map(|order| {
			plan.order(*order).map(|stages| {
				(*order, stages.iter().copied().map(stage_kind).collect::<Box<[WriteStageKind]>>())
			})
		})
		.collect()
}

/// The stages one statement binds, in order.
///
/// A form with one order binds that order. A form with a creating order and an updating order binds
/// the creating order's stages. A stage that only the updating order holds goes right after the
/// stage before it in the creating order. A stage that both orders hold keeps its place in the
/// creating order, so the updating order may run it out of the run's order. Each stage appears
/// once, so a clause a stage evaluates is bound once, whichever order a record takes. Every order
/// puts all `Compute` stages before all `Apply` stages. A stage the updating order adds takes its
/// place among stages with the same mark, so the marks of the run split it at most once.
///
/// A view row's three orders run the same stages, one action apart. Its run holds the stages of one
/// kind together, in the order of the orders that hold them (create, update, delete), so the run
/// keeps the order of the stage kinds.
pub(crate) fn stage_run(form: PlanForm, facts: WriteFacts) -> Vec<WriteStageKind> {
	if form == PlanForm::View {
		return view_row_run(facts);
	}
	let mut run: Vec<WriteStageKind> = Vec::new();
	for (_, stages) in orders(form, facts) {
		let mut after: Option<usize> = None;
		for stage in stages.iter().copied() {
			let at = match run.iter().position(|held| *held == stage) {
				Some(at) => at,
				None => {
					let at = after.map_or(0, |previous| previous + 1);
					run.insert(at, stage);
					at
				}
			};
			after = Some(at);
		}
	}
	run
}

/// The run of a view row's write ([`stage_run`]): the stage at each position of the three orders,
/// taken from each order in turn.
fn view_row_run(facts: WriteFacts) -> Vec<WriteStageKind> {
	let built = orders(PlanForm::View, facts);
	let longest = built.iter().map(|(_, stages)| stages.len()).max().unwrap_or(0);
	let mut run: Vec<WriteStageKind> = Vec::new();
	for at in 0..longest {
		for (_, stages) in &built {
			if let Some(stage) = stages.get(at)
				&& !run.contains(stage)
			{
				run.push(*stage);
			}
		}
	}
	run
}

/// The action a stage maintains what a table stores for, or `None` for a stage every action runs
/// alike.
fn action_of(stage: WriteStageKind) -> Option<Action> {
	match stage {
		WriteStageKind::ProcessTableViews(action)
		| WriteStageKind::ProcessTableEvents(action)
		| WriteStageKind::ProcessTableLives(action) => Some(action),
		WriteStageKind::CheckRecordUserAccess
		| WriteStageKind::CheckRecordExists
		| WriteStageKind::CheckTableNotView
		| WriteStageKind::CheckTableNotLightweight(_)
		| WriteStageKind::CheckTableType(_)
		| WriteStageKind::CheckPermissionsQuickCreate
		| WriteStageKind::CheckUpdatePermissions
		| WriteStageKind::CheckDeletePermissions
		| WriteStageKind::CheckCreatePermissions
		| WriteStageKind::RecheckUpdatePermissions
		| WriteStageKind::CheckWhereCondition
		| WriteStageKind::ComputeInputData
		| WriteStageKind::CheckDataFields
		| WriteStageKind::ProcessRecordData
		| WriteStageKind::ProcessMergeData
		| WriteStageKind::DefaultRecordData
		| WriteStageKind::GenerateRecordId
		| WriteStageKind::ProcessTableFields
		| WriteStageKind::CleanupTableFields
		| WriteStageKind::CleanupTableReferences
		| WriteStageKind::ClearRecordData
		| WriteStageKind::StoreRecordData
		| WriteStageKind::StoreEdgesData
		| WriteStageKind::StoreInlineAdjacencyData
		| WriteStageKind::StoreIndexData
		| WriteStageKind::ReclaimDocId
		| WriteStageKind::PurgeRecordData
		| WriteStageKind::MaterialiseObservedFields
		| WriteStageKind::ProcessTableReferences
		| WriteStageKind::ProcessChangefeeds
		| WriteStageKind::CheckSelectPermissions(_)
		| WriteStageKind::OutputWrite => None,
	}
}

/// The stages the write of a view row whose body is `body` runs for a row its view's maintenance
/// settled `action` for: the stages of the first order, at the positions it names in `run`, that
/// carry no action or carry `action` ([`WriteBranching::ByAction`]). Answers [`IrError::Invariant`]
/// for a body that does not branch by action, or a position `run` has no stage at.
pub(crate) fn view_row_order(
	body: &WriteBody,
	run: &[WriteStageKind],
	action: Action,
) -> Result<Vec<WriteStageKind>, IrError> {
	if body.branching != WriteBranching::ByAction {
		return Err(invariant("the orders of a write that does not branch by action"));
	}
	let mut order = Vec::with_capacity(body.first.len());
	for at in &body.first {
		let stage = run
			.get(usize::from(*at))
			.copied()
			.ok_or_else(|| invariant("a position beyond the stages of a write"))?;
		if action_of(stage).is_none_or(|held| held == action) {
			order.push(stage);
		}
	}
	Ok(order)
}

/// The body of a write of `form`: how it chooses between its orders, and the positions in `run` of
/// the stages each order runs.
///
/// `run` is [`stage_run`] of the same `facts`, so it holds every stage of every order. `cond` is
/// whether the statement has a `WHERE` condition, which decides whether a table among the subjects
/// names a record to create. A view row's body branches by action and names every stage of `run`
/// in its first order, each order of the form being the stages of one action
/// ([`view_row_order`]). An order that holds a stage `run` does not is an invariant failure.
pub(crate) fn write_body(
	form: PlanForm,
	facts: WriteFacts,
	run: &[WriteStageKind],
	cond: bool,
) -> Result<WriteBody, IrError> {
	let plan = WritePlan::build(WriteFacts {
		form,
		..facts
	});
	let positions = |order: WriteOrder| -> Result<Box<[u16]>, IrError> {
		let stages =
			plan.order(order).ok_or_else(|| invariant("a write form holds no such order"))?;
		stages
			.iter()
			.map(|stage| {
				let kind = stage_kind(*stage);
				run.iter()
					.position(|held| *held == kind)
					.and_then(|at| u16::try_from(at).ok())
					.ok_or_else(|| invariant("an order of a write holds a stage its run lacks"))
			})
			.collect()
	};
	let (branching, first, second) = match plan.branching() {
		Branching::Sole(order) => (WriteBranching::Sole, positions(order)?, Box::default()),
		Branching::UpFront {
			creating,
			updating,
		} => (WriteBranching::UpFront, positions(creating)?, positions(updating)?),
		Branching::Retry {
			creating,
			updating,
			policy,
		} => (
			WriteBranching::Retry(WriteRetry {
				savepoint_after: u16::try_from(policy.savepoint_after)
					.map_err(|_| invariant("a savepoint position beyond the stages of an order"))?,
				retry_conflict: policy.retry_conflict,
				skip_failures: policy.skip_failures,
				carry_schema_failure: policy.carry_schema_failure,
			}),
			positions(creating)?,
			positions(updating)?,
		),
		Branching::ByAction => {
			let every = (0..run.len())
				.map(|at| u16::try_from(at).map_err(|_| invariant("a stage beyond a write's run")))
				.collect::<Result<Box<[u16]>, IrError>>()?;
			let body = WriteBody {
				branching: WriteBranching::ByAction,
				first: every,
				second: Box::default(),
				deferable: false,
			};
			for (order, action) in [
				(WriteOrder::ViewCreate, Action::Create),
				(WriteOrder::ViewUpdate, Action::Update),
				(WriteOrder::ViewDelete, Action::Delete),
			] {
				let stated = positions(order)?
					.iter()
					.map(|at| run[usize::from(*at)])
					.collect::<Vec<WriteStageKind>>();
				if view_row_order(&body, run, action)? != stated {
					return Err(invariant("a view row's run does not hold its orders by action"));
				}
			}
			return Ok(body);
		}
	};
	// `Statement::is_deferable`: a table names a record to create for `CREATE`, and for `UPSERT`
	// with no condition. Every other form reads the records stored at the table.
	let deferable = matches!(form, PlanForm::Create) || (matches!(form, PlanForm::Upsert) && !cond);
	Ok(WriteBody {
		branching,
		first,
		second,
		deferable,
	})
}

/// The subject loop that yields the subjects of the write `write`. It is the write loop that the
/// write's first operand is the result of, or the write loop held by the `phys.pipeline` of that
/// operand, which is where placement leaves a loop that fans out. `None` when the first operand is
/// neither.
pub(crate) fn write_loop(ir: &Ir, write: &OpData) -> Option<OpId> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(*write.operands.first()?)?.def
	else {
		return None;
	};
	let is_write_loop = |op: OpId| match ir.op(op).map(|data| &data.kind) {
		Some(OpKind::Logical(LogOp::Loop {
			kind,
			..
		})) => *kind == LoopKind::Write,
		Some(OpKind::Phys(PhysOp::Loop {
			kind,
			..
		})) => *kind == LoopKind::Write,
		Some(OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_)) | None => false,
	};
	let held = match &ir.op(op)?.kind {
		OpKind::Phys(PhysOp::Pipeline(_)) => {
			let region = *ir.op(op)?.regions.first()?;
			ops_in(ir, region).find(|inner| is_write_loop(*inner))?
		}
		OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_) => op,
	};
	is_write_loop(held).then_some(held)
}

/// The stage ops directly in the body of the subject loop of the write `write` ([`write_loop`]), in
/// the order they run. `None` when the write's first operand is not the result of a write loop.
pub(crate) fn write_stage_ops(ir: &Ir, write: &OpData) -> Option<Vec<(OpId, WriteStageKind)>> {
	let looped = ir.op(write_loop(ir, write)?)?;
	let body = *looped.regions.first()?;
	Some(
		ops_in(ir, body)
			.filter_map(|op| match &ir.op(op)?.kind {
				OpKind::Logical(LogOp::WriteStage {
					stage,
					..
				})
				| OpKind::Phys(PhysOp::WriteStage {
					stage,
					..
				}) => Some((op, *stage)),
				OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_) => None,
			})
			.collect(),
	)
}

/// The stages [`write_stage_ops`] finds, without the ops that hold them.
pub(crate) fn write_run(ir: &Ir, write: &OpData) -> Option<Vec<WriteStageKind>> {
	write_stage_ops(ir, write).map(|ops| ops.into_iter().map(|(_, stage)| stage).collect())
}

/// Why `body` does not describe a write whose loop body holds `stages` stage ops, or `None` when it
/// does.
///
/// Each order names distinct positions inside the stage ops. The first order's positions ascend and
/// the order is never empty. A form with two orders names a second one, and a form with one order
/// names none. A savepoint falls within the creating order. Every stage op belongs to an order. The
/// second order's positions need not ascend, because an updating order may run stages it shares
/// with the creating order in a different relative order.
pub(crate) fn body_fault(body: &WriteBody, stages: usize) -> Option<&'static str> {
	let within = |order: &[u16]| order.iter().all(|position| usize::from(*position) < stages);
	let distinct = |order: &[u16]| {
		order.iter().enumerate().all(|(index, position)| !order[..index].contains(position))
	};
	if !within(&body.first) || !within(&body.second) {
		return Some("an order of a write names a position beyond its stages");
	}
	if !body.first.windows(2).all(|pair| pair[0] < pair[1]) {
		return Some("the first order of a write names positions that do not ascend");
	}
	if !distinct(&body.second) {
		return Some("an order of a write names a position twice");
	}
	if body.first.is_empty() {
		return Some("a write names no stage for its first order");
	}
	match body.branching {
		WriteBranching::Sole | WriteBranching::ByAction => {
			if !body.second.is_empty() {
				return Some("a write with one order names a second");
			}
		}
		WriteBranching::UpFront | WriteBranching::Retry(_) => {
			if body.second.is_empty() {
				return Some("a write with two orders names no stage for the second");
			}
		}
	}
	if let WriteBranching::Retry(retry) = body.branching
		&& usize::from(retry.savepoint_after) > body.first.len()
	{
		return Some("a savepoint of a write falls beyond its creating order");
	}
	let covered = |at: usize| {
		body.first.iter().chain(body.second.iter()).any(|position| usize::from(*position) == at)
	};
	if !(0..stages).all(covered) {
		return Some("a stage of a write belongs to no order");
	}
	None
}

/// The `PERMISSIONS FOR` clauses of a written table, as the write states them and as its gate
/// stages host them.
pub(crate) struct TableGates {
	/// What each clause says, as the write's attributes carry it.
	pub(crate) gates: WriteGates,
	/// The predicate of each clause that is one, in logical form, by the clause it belongs to.
	pub(crate) create: Option<LogicalExpr>,
	pub(crate) update: Option<LogicalExpr>,
	pub(crate) delete: Option<LogicalExpr>,
	pub(crate) select: Option<LogicalExpr>,
	/// The `COMPUTED` fields each predicate derives, with their bodies, by the clause they
	/// belong to.
	pub(crate) derivations: GateDerivations,
}

/// The derivations of the predicate of each `PERMISSIONS FOR` clause.
#[derive(Default)]
pub(crate) struct GateDerivations {
	pub(crate) create: Vec<Derivation>,
	pub(crate) update: Vec<Derivation>,
	pub(crate) delete: Vec<Derivation>,
	pub(crate) select: Vec<Derivation>,
}

/// A `COMPUTED` field a stage derives, with the body that derives it.
pub(crate) struct Derivation {
	/// The field and the facts of its definition a derivation reads.
	pub(crate) field: DerivedField,
	/// The `AUTH LIMIT` the body runs under, when it narrows the caller.
	pub(crate) auth: Option<crate::iam::AuthLimit>,
	/// The body, in logical form.
	pub(crate) body: LogicalExpr,
}

impl TableGates {
	/// Whether any clause is a predicate.
	pub(crate) fn has_predicate(&self) -> bool {
		self.create.is_some()
			|| self.update.is_some()
			|| self.delete.is_some()
			|| self.select.is_some()
	}

	/// The fields the predicate of the gate stage `stage` derives, in the order its regions host
	/// them. None for a stage that is no permission gate.
	pub(crate) fn derived(&self, stage: WriteStageKind) -> &[Derivation] {
		match stage {
			WriteStageKind::CheckCreatePermissions => &self.derivations.create,
			WriteStageKind::CheckUpdatePermissions | WriteStageKind::RecheckUpdatePermissions => {
				&self.derivations.update
			}
			WriteStageKind::CheckDeletePermissions => &self.derivations.delete,
			WriteStageKind::CheckSelectPermissions(_) => &self.derivations.select,
			WriteStageKind::CheckRecordUserAccess
			| WriteStageKind::CheckRecordExists
			| WriteStageKind::CheckTableNotView
			| WriteStageKind::CheckTableNotLightweight(_)
			| WriteStageKind::CheckTableType(_)
			| WriteStageKind::CheckPermissionsQuickCreate
			| WriteStageKind::CheckWhereCondition
			| WriteStageKind::ComputeInputData
			| WriteStageKind::CheckDataFields
			| WriteStageKind::ProcessRecordData
			| WriteStageKind::ProcessMergeData
			| WriteStageKind::DefaultRecordData
			| WriteStageKind::GenerateRecordId
			| WriteStageKind::ProcessTableFields
			| WriteStageKind::CleanupTableFields
			| WriteStageKind::CleanupTableReferences
			| WriteStageKind::ClearRecordData
			| WriteStageKind::StoreRecordData
			| WriteStageKind::StoreEdgesData
			| WriteStageKind::StoreInlineAdjacencyData
			| WriteStageKind::StoreIndexData
			| WriteStageKind::ReclaimDocId
			| WriteStageKind::PurgeRecordData
			| WriteStageKind::MaterialiseObservedFields
			| WriteStageKind::ProcessTableReferences
			| WriteStageKind::ProcessTableViews(_)
			| WriteStageKind::ProcessTableEvents(_)
			| WriteStageKind::ProcessTableLives(_)
			| WriteStageKind::ProcessChangefeeds
			| WriteStageKind::OutputWrite => &[],
		}
	}

	/// The derivations of every clause, as the attribute of the write states them.
	pub(crate) fn derived_attr(&self, observed: Option<&[Derivation]>) -> WriteDerived {
		let fields = |list: &[Derivation]| -> Box<[DerivedField]> {
			list.iter().map(|derivation| derivation.field.clone()).collect()
		};
		WriteDerived {
			create: fields(&self.derivations.create),
			update: fields(&self.derivations.update),
			delete: fields(&self.derivations.delete),
			select: fields(&self.derivations.select),
			observed: observed.map(fields),
		}
	}

	/// The predicate the gate stage `stage` hosts, and the clause kind it judges, or `None` for a
	/// stage that is no permission gate.
	pub(crate) fn hosted(
		&self,
		stage: WriteStageKind,
	) -> Option<(Option<&LogicalExpr>, GuardKind)> {
		match stage {
			WriteStageKind::CheckCreatePermissions => {
				Some((self.create.as_ref(), GuardKind::Write))
			}
			WriteStageKind::CheckUpdatePermissions | WriteStageKind::RecheckUpdatePermissions => {
				Some((self.update.as_ref(), GuardKind::Write))
			}
			WriteStageKind::CheckDeletePermissions => {
				Some((self.delete.as_ref(), GuardKind::Write))
			}
			WriteStageKind::CheckSelectPermissions(_) => {
				Some((self.select.as_ref(), GuardKind::Read))
			}
			WriteStageKind::CheckRecordUserAccess
			| WriteStageKind::CheckRecordExists
			| WriteStageKind::CheckTableNotView
			| WriteStageKind::CheckTableNotLightweight(_)
			| WriteStageKind::CheckTableType(_)
			| WriteStageKind::CheckPermissionsQuickCreate
			| WriteStageKind::CheckWhereCondition
			| WriteStageKind::ComputeInputData
			| WriteStageKind::CheckDataFields
			| WriteStageKind::ProcessRecordData
			| WriteStageKind::ProcessMergeData
			| WriteStageKind::DefaultRecordData
			| WriteStageKind::GenerateRecordId
			| WriteStageKind::ProcessTableFields
			| WriteStageKind::CleanupTableFields
			| WriteStageKind::CleanupTableReferences
			| WriteStageKind::ClearRecordData
			| WriteStageKind::StoreRecordData
			| WriteStageKind::StoreEdgesData
			| WriteStageKind::StoreInlineAdjacencyData
			| WriteStageKind::StoreIndexData
			| WriteStageKind::ReclaimDocId
			| WriteStageKind::PurgeRecordData
			| WriteStageKind::MaterialiseObservedFields
			| WriteStageKind::ProcessTableReferences
			| WriteStageKind::ProcessTableViews(_)
			| WriteStageKind::ProcessTableEvents(_)
			| WriteStageKind::ProcessTableLives(_)
			| WriteStageKind::ProcessChangefeeds
			| WriteStageKind::OutputWrite => None,
		}
	}
}

/// The permission clauses `table` stores, read at the catalog version the request is bound to.
///
/// A clause is never read as absent by default. A write that names no one table states every
/// clause [`GateClause::Dynamic`]. A table the catalog does not define is [`GateClause::Absent`]
/// throughout. A catalog that cannot answer, because it changed before the statement or holds
/// no definition yet, states every clause [`GateClause::LateBound`], so that a lowering refuses it
/// and never judges a permission the plan did not read. A predicate states whether it may read a
/// `COMPUTED` field ([`GateClause::Predicate`]) by the test of [`derivations_for`], which also
/// yields the fields it derives and their bodies.
pub(crate) fn gates_of(catalog: &dyn SemanticCatalog, table: Option<&TableName>) -> TableGates {
	let all = |clause| TableGates {
		gates: WriteGates {
			create: clause,
			update: clause,
			delete: clause,
			select: clause,
		},
		create: None,
		update: None,
		delete: None,
		select: None,
		derivations: GateDerivations::default(),
	};
	let Some(table) = table else {
		return all(GateClause::Dynamic);
	};
	let snapshot = match catalog.table(table) {
		CatalogAnswer::Found(snapshot) => snapshot,
		CatalogAnswer::Absent => return all(GateClause::Absent),
		CatalogAnswer::NotLoaded | CatalogAnswer::Unknown => return all(GateClause::LateBound),
	};
	let Some(definition) = snapshot.definition.as_ref() else {
		return all(GateClause::Absent);
	};
	let clause = |kind| match definition.permission_logical(kind) {
		Ok(LogicalPermission::Full) => (GateClause::Full, None, Vec::new()),
		Ok(LogicalPermission::None) => (GateClause::None, None, Vec::new()),
		Ok(LogicalPermission::Specific(predicate)) => {
			let stored = match kind {
				PermissionKind::Select => &definition.permissions.select,
				PermissionKind::Create => &definition.permissions.create,
				PermissionKind::Update => &definition.permissions.update,
				PermissionKind::Delete => &definition.permissions.delete,
			};
			match derivations_for(&snapshot.fields, stored) {
				Some(derived) => (
					GateClause::Predicate {
						derives: !derived.is_empty(),
					},
					Some(predicate),
					derived,
				),
				None => (GateClause::Unlowerable, None, Vec::new()),
			}
		}
		Err(_) => (GateClause::Unlowerable, None, Vec::new()),
	};
	let (create, create_predicate, create_derived) = clause(PermissionKind::Create);
	let (update, update_predicate, update_derived) = clause(PermissionKind::Update);
	let (delete, delete_predicate, delete_derived) = clause(PermissionKind::Delete);
	let (select, select_predicate, select_derived) = clause(PermissionKind::Select);
	TableGates {
		gates: WriteGates {
			create,
			update,
			delete,
			select,
		},
		create: create_predicate,
		update: update_predicate,
		delete: delete_predicate,
		select: select_predicate,
		derivations: GateDerivations {
			create: create_derived,
			update: update_derived,
			delete: delete_derived,
			select: select_derived,
		},
	}
}

/// The `COMPUTED` fields that a write gate's stored clause `stored` derives onto its copy of an
/// image, with their bodies, in the table's derivation order.
///
/// A clause whose reads are all named, and none of them is a field of the record, derives nothing.
/// Any other clause derives the computed fields it names. If its reads cannot all be named, it
/// derives every computed field, because an opaque construct may read any field. Each derived field
/// brings in the computed fields it reads in turn. `None` when a computed field of the table has no
/// logical form or is not named by exactly one field, since no derivation can key it.
fn derivations_for(fields: &[FieldDefinition], stored: &Permission) -> Option<Vec<Derivation>> {
	let Permission::Specific(clause) = stored else {
		return Some(Vec::new());
	};
	let deps = crate::expr::computed_deps::extract_computed_deps(clause);
	if deps.is_complete && deps.fields.is_empty() {
		return Some(Vec::new());
	}
	let reads = deps.is_complete.then_some(deps.fields);
	derivations_of(fields, reads.as_deref())
}

/// The `COMPUTED` fields of `fields` that a clause reading the fields `reads` derives (every field
/// of the record when `reads` is `None`), with their bodies. They come in the order the table's
/// view derives them: the computed fields the clause reads, and the computed fields those read in
/// turn, each after the fields it reads.
///
/// `None` when a computed field has no logical form or is not named by exactly one field.
pub(crate) fn derivations_of(
	fields: &[FieldDefinition],
	reads: Option<&[String]>,
) -> Option<Vec<Derivation>> {
	let mut computed = Vec::new();
	let mut order: Vec<(String, Vec<String>)> = Vec::new();
	for field in fields {
		let (Some(stored), Some(body)) = (field.computed.as_ref(), field.computed_logical()) else {
			continue;
		};
		let body = body.ok()?.clone();
		let name = field.name.field_name()?;
		order.push((
			name.to_owned(),
			crate::expr::computed_deps::extract_computed_deps(stored).fields,
		));
		let auth = narrowing_auth_limit(&field.auth_limit).ok()?;
		computed.push(Derivation {
			field: DerivedField {
				name: Strand::new(name),
				kind: field.field_kind.clone(),
				caller_bound: needs_caller(&body),
			},
			auth,
			body,
		});
	}
	let needed = reads.map(|reads| needed_fields(&order, reads));
	let sorted = crate::expr::computed_deps::topological_sort_computed_fields(&order);
	let mut slots: Vec<Option<Derivation>> = computed.into_iter().map(Some).collect();
	Some(
		sorted
			.into_iter()
			.filter_map(|index| slots.get_mut(index)?.take())
			.filter(|derivation| {
				needed.as_ref().is_none_or(|needed| {
					needed.iter().any(|name| name == derivation.field.name.as_str())
				})
			})
			.collect(),
	)
}

/// The names of the `COMPUTED` fields that `reads` reaches through `order`. Each field's name is
/// listed with the fields its body reads: the computed fields among `reads`, and every computed
/// field one of them reads, transitively.
fn needed_fields(order: &[(String, Vec<String>)], reads: &[String]) -> Vec<String> {
	let mut needed: Vec<String> = Vec::new();
	let mut pending: Vec<&str> = reads.iter().map(String::as_str).collect();
	while let Some(name) = pending.pop() {
		if needed.iter().any(|seen| seen == name) {
			continue;
		}
		if let Some((name, deps)) = order.iter().find(|(field, _)| field == name) {
			needed.push(name.clone());
			pending.extend(deps.iter().map(String::as_str));
		}
	}
	needed
}

/// Whether `expr` holds something lowered for the principal that runs it: a statement, a closure,
/// or a call of a user-defined function.
#[allow(clippy::wildcard_enum_match_arm)]
pub(crate) fn needs_caller(expr: &LogicalExpr) -> bool {
	expr.any(|node| match node {
		LogicalExpr::Subquery(_) | LogicalExpr::Closure(_) => true,
		LogicalExpr::Call(call) => matches!(call.receiver, crate::expr::Function::Custom(_)),
		_ => false,
	})
}

/// Every `COMPUTED` field of `table`, in derivation order. `MaterialiseObservedFields` derives them
/// onto both images of an observed record. `None` when the catalog does not state them (a write
/// that names no one table, or an answer the catalog cannot give), or when a computed field has no
/// logical form or no single name.
///
/// The fields that a keyed table's id components name are computed definitions of the table's
/// fields, so they are derived here like any other.
pub(crate) fn observed_of(
	catalog: &dyn SemanticCatalog,
	table: Option<&TableName>,
) -> Option<Vec<Derivation>> {
	let table = table?;
	match catalog.table(table) {
		CatalogAnswer::Found(snapshot) => match snapshot.definition {
			Some(_) => derivations_of(&snapshot.fields, None),
			None => Some(Vec::new()),
		},
		CatalogAnswer::Absent => Some(Vec::new()),
		CatalogAnswer::NotLoaded | CatalogAnswer::Unknown => None,
	}
}

/// What the field declarations of `table` say about the images of its records, read at the catalog
/// version the request is bound to.
///
/// A write that names no one table states [`ImageView::Dynamic`]. A catalog that cannot answer
/// states [`ImageView::LateBound`]. A lowering refuses both, so it never shows or reads an image
/// the plan did not describe. A table the catalog does not define has no fields: its images are
/// plain, it is not a keyed table, and its `id` has no `DEFAULT`. A table's view derives when any
/// field is `COMPUTED`. It cuts for a checked caller when any field's select permission is not
/// `FULL`. Both are facts of the table's own fields. Whether the caller is checked is a fact of the
/// frame the program is lowered for.
pub(crate) fn image_of(catalog: &dyn SemanticCatalog, table: Option<&TableName>) -> WriteImage {
	let stated = |view| WriteImage {
		view,
		keyed: false,
		id_default: false,
	};
	let Some(table) = table else {
		return stated(ImageView::Dynamic);
	};
	let snapshot = match catalog.table(table) {
		CatalogAnswer::Found(snapshot) => snapshot,
		CatalogAnswer::Absent => return stated(ImageView::Plain),
		CatalogAnswer::NotLoaded | CatalogAnswer::Unknown => {
			return stated(ImageView::LateBound);
		}
	};
	if snapshot.definition.is_none() {
		return stated(ImageView::Plain);
	}
	WriteImage {
		view: view_of(&snapshot.fields),
		keyed: snapshot.key.as_ref().is_some_and(|key| key.origin == KeyOrigin::IdField),
		id_default: declares_id_default(&snapshot.fields),
	}
}

/// What the table `table` stores that the stages of a write run, from the catalog's answers.
///
/// A write that names no one table states nothing ([`Stored::Dynamic`]). A table the catalog does
/// not define stores nothing and is not `SCHEMAFULL`, as the write defines it on demand. An answer
/// the catalog cannot give (it changed before the statement, or the table is not loaded) states
/// [`Stored::LateBound`]. A lowering refuses that and never reads a definition the plan did not
/// describe. `catalog_fixed` is whether the statement cannot change the catalog
/// ([`catalog_fixed_of`]).
pub(crate) fn stored_of(
	catalog: &dyn SemanticCatalog,
	table: Option<&TableName>,
	catalog_fixed: bool,
) -> WriteStored {
	let Some(table) = table else {
		return WriteStored {
			catalog_fixed,
			..WriteStored::default()
		};
	};
	let all = |kind| WriteStored {
		table: Some(table.clone()),
		schemafull: false,
		catalog_fixed,
		fields: kind,
		references: kind,
		indexes: kind,
		events: kind,
		views: kind,
		referenced: kind,
		schema: WriteSchema::default(),
		reference_fields: Box::default(),
		index_set: WriteIndexes::default(),
		view_set: WriteViews::default(),
		repair_set: WriteRepairs::default(),
		image_layers: ImageLayers::default(),
	};
	let snapshot = match catalog.table(table) {
		CatalogAnswer::Found(snapshot) => snapshot,
		CatalogAnswer::Absent => return all(Stored::Nothing),
		CatalogAnswer::NotLoaded | CatalogAnswer::Unknown => return all(Stored::LateBound),
	};
	let Some(definition) = &snapshot.definition else {
		return all(Stored::Nothing);
	};
	let of = |any: bool| {
		if any {
			Stored::Declared
		} else {
			Stored::Nothing
		}
	};
	let events = match catalog.events(table) {
		CatalogAnswer::Found(events) => of(!events.is_empty()),
		CatalogAnswer::Absent => Stored::Nothing,
		CatalogAnswer::NotLoaded | CatalogAnswer::Unknown => Stored::LateBound,
	};
	let views = match catalog.views(table) {
		CatalogAnswer::Found(views) => of(!views.is_empty()),
		CatalogAnswer::Absent => Stored::Nothing,
		CatalogAnswer::NotLoaded | CatalogAnswer::Unknown => Stored::LateBound,
	};
	WriteStored {
		table: Some(table.clone()),
		schemafull: definition.schemafull,
		catalog_fixed,
		fields: of(!snapshot.fields.is_empty()),
		references: of(snapshot.fields.iter().any(|field| field.reference.is_some())),
		indexes: of(!snapshot.indexes.is_empty()),
		events,
		views,
		referenced: Stored::Nothing,
		schema: WriteSchema::default(),
		reference_fields: snapshot
			.fields
			.iter()
			.filter(|field| field.reference.is_some())
			.map(|field| field.name.clone())
			.collect(),
		index_set: WriteIndexes::default(),
		view_set: WriteViews::default(),
		repair_set: WriteRepairs::default(),
		image_layers: ImageLayers::default(),
	}
}

/// Whether the statement `write` cannot change the catalog. It is not a `DELETE`, since a delete
/// clears the references to what it deletes in any table. Nothing it names or runs may define or
/// remove a definition, and it cannot define or remove a definition it cannot name.
///
/// A write that cannot change the catalog meets, for every record, the definitions its plan read.
/// So a table that stores no index or event has no stage to run for them.
pub(crate) fn catalog_fixed_of(write: &LogicalWrite, registry: &FunctionRegistry) -> bool {
	!matches!(write.statement, LogicalWriteStatement::Delete(_))
		&& !crate::exec::logical::footprint::of(&LogicalOp::Write(write.clone()), registry)
			.writes_catalog()
}

/// What a table's field view does to an image, from its field definitions.
pub(crate) fn view_of(fields: &[FieldDefinition]) -> ImageView {
	if fields.iter().any(|field| field.computed.is_some()) {
		ImageView::Derived
	} else if fields.iter().any(|field| !matches!(field.select_permission, Permission::Full)) {
		ImageView::Cut
	} else {
		ImageView::Plain
	}
}

/// Whether the `id` field among `fields` declares a `DEFAULT` that applies when no id is supplied.
pub(crate) fn declares_id_default(fields: &[FieldDefinition]) -> bool {
	crate::doc::id_field_index(fields)
		.and_then(|index| fields.get(index))
		.is_some_and(|field| matches!(field.default, DefineDefault::Set(_)))
}

/// A clause of the field schema whose body is a region of the stage op that applies it.
pub(crate) struct SchemaRegion {
	/// The kind of the region.
	pub(crate) kind: RegionKind,
	/// The frame the body runs in, under the field's `AUTH LIMIT`.
	pub(crate) frame: EvalFrame,
	/// The body, in logical form.
	pub(crate) body: LogicalExpr,
	/// Whether the region binds the names of a field's place (`$value`, `$after`, `$before` and
	/// `$input`) to block arguments after the row, as the clause of a field does.
	pub(crate) place: bool,
}

/// What a write's table declares for the stages that apply its fields. It holds the facts the write
/// states, the regions `ProcessTableFields` hosts (in the order [`WriteSchema::region_counts`]
/// lists their kinds), and the region `GenerateRecordId` hosts for an `id` that declares a
/// `DEFAULT`.
#[derive(Default)]
pub(crate) struct TableSchema {
	pub(crate) facts: WriteSchema,
	pub(crate) regions: Vec<SchemaRegion>,
	pub(crate) id_default: Option<SchemaRegion>,
}

/// The field schema of `table`, from the catalog's answers at the version the request is bound to.
///
/// A write that names no one table, a table the catalog does not define, an answer the catalog
/// cannot give, and a table with no fields all state nothing. The stage that applies the schema
/// reads the write's [`Stored`] facts first. Otherwise the fields are processed as follows. They
/// come in the order they are produced ([`crate::doc::field_eval_order`]). A field with no
/// `DEFAULT` takes a static `VALUE` as its default. A `COMPUTED` field that another field's clause
/// reads is derived at its place. The predicate of a field's `create` and `update` permission comes
/// with the computed fields it reads. A clause with no logical form, one that holds a write no
/// operator runs, and a computed field not named by exactly one field state no step
/// ([`WriteSchema::unlowerable`]).
pub(crate) fn schema_of(
	catalog: &dyn SemanticCatalog,
	registry: &FunctionRegistry,
	table: Option<&TableName>,
) -> TableSchema {
	let Some(table) = table else {
		return TableSchema::default();
	};
	let CatalogAnswer::Found(snapshot) = catalog.table(table) else {
		return TableSchema::default();
	};
	if snapshot.definition.is_none() {
		return TableSchema::default();
	}
	schema_over(&snapshot.fields, registry)
}

/// The field schema of a table whose field definitions are `fields`: see [`schema_of`]. A table
/// that declares no field states nothing.
pub(crate) fn schema_over(fields: &[FieldDefinition], registry: &FunctionRegistry) -> TableSchema {
	if fields.is_empty() {
		return TableSchema::default();
	}
	let id_field = crate::doc::id_field_index(fields);
	let computed = fields
		.iter()
		.filter(|fd| fd.computed.is_some())
		.map(|fd| fd.name.field_name().map(Strand::new))
		.collect::<Option<Box<[Strand]>>>();
	let mut facts = WriteSchema {
		fields: fields.iter().map(|fd| Strand::new(fd.name.to_raw_string())).collect(),
		id_field: id_field.and_then(|index| u16::try_from(index).ok()),
		computed: computed.clone().unwrap_or_default(),
		unlowerable: computed.is_none()
			|| id_field.is_some_and(|index| index > usize::from(u16::MAX)),
		..WriteSchema::default()
	};
	let order = crate::doc::field_eval_order(fields);
	if let Some(cycle) = &order.cycle {
		facts.cycle = Some(cycle.join(", ").into());
		return TableSchema {
			facts,
			..TableSchema::default()
		};
	}
	let id_default = id_default_of(fields);
	let steps = schema_steps(fields, &order, registry);
	let (Some((steps, regions)), false, false) =
		(steps, facts.unlowerable, declares_id_default(fields) && id_default.is_none())
	else {
		facts.unlowerable = true;
		return TableSchema {
			facts,
			..TableSchema::default()
		};
	};
	facts.steps = steps;
	facts.id_default_bound = id_default.as_ref().is_some_and(|region| needs_caller(&region.body));
	TableSchema {
		facts,
		regions,
		id_default,
	}
}

/// The `DEFAULT` of the `id` field among `fields` as the region `GenerateRecordId` hosts, when it
/// declares one that applies when no id is supplied.
fn id_default_of(fields: &[FieldDefinition]) -> Option<SchemaRegion> {
	let field = fields.get(crate::doc::id_field_index(fields)?)?;
	if !matches!(field.default, DefineDefault::Set(_)) {
		return None;
	}
	let body = field.default_logical()?.ok()?;
	clause_region(field, RegionKind::FieldFrame(FieldRole::Default), body, true)
}

/// The region of a clause of `field` with body `body`: in the caller's mode under the field's
/// `AUTH LIMIT`. `None` when the body holds a write no operator runs or the limit does not
/// convert.
fn clause_region(
	field: &FieldDefinition,
	kind: RegionKind,
	body: &LogicalExpr,
	place: bool,
) -> Option<SchemaRegion> {
	if crate::exec::logical::unplannable_write_in_expr(body).is_some() {
		return None;
	}
	let auth = narrowing_auth_limit(&field.auth_limit).ok()?;
	Some(SchemaRegion {
		kind,
		frame: EvalFrame {
			auth,
			..EvalFrame::caller()
		},
		body: body.clone(),
		place,
	})
}

/// The regions of the field schema by kind, in the order the stage op hosts them.
#[derive(Default)]
struct Groups {
	default: Vec<SchemaRegion>,
	value: Vec<SchemaRegion>,
	assert: Vec<SchemaRegion>,
	permission: Vec<SchemaRegion>,
	derive: Vec<SchemaRegion>,
	predicate_derive: Vec<SchemaRegion>,
}

/// The steps of the field schema over `fields` in the order `order` produces them, with their
/// regions in the order of [`WriteSchema::region_counts`]; `None` when a clause of a field has no
/// form the lowering can run.
fn schema_steps(
	fields: &[FieldDefinition],
	order: &crate::doc::FieldEvalOrder,
	registry: &FunctionRegistry,
) -> Option<(Box<[FieldStep]>, Vec<SchemaRegion>)> {
	let mut groups = Groups::default();
	let mut steps = Vec::with_capacity(order.order.len());
	for &index in &order.order {
		let field = fields.get(index)?;
		if field.computed.is_some() {
			// A computed field that another field's clause reads is derived here. The others are
			// left to the read side.
			if !order.required_computed.contains(&index) {
				continue;
			}
			let body = field.computed_logical()?.ok()?;
			if crate::exec::logical::unplannable_write_in_expr(body).is_some() {
				return None;
			}
			let auth = narrowing_auth_limit(&field.auth_limit).ok()?;
			groups.derive.push(SchemaRegion {
				kind: RegionKind::FieldFrame(FieldRole::Computed),
				frame: EvalFrame::computed(auth),
				body: body.clone(),
				place: false,
			});
			steps.push(FieldStep::Derive(DerivedField {
				name: Strand::new(field.name.field_name()?),
				kind: field.field_kind.clone(),
				caller_bound: needs_caller(body),
			}));
			continue;
		}
		let default = if field.default.expression().is_some() {
			field.default_logical()
		} else {
			match (field.value.as_ref(), field.value_body.as_ref()) {
				(Some(expr), Some(body)) if expr.is_static() => Some(body.logical()),
				_ => None,
			}
		};
		let mut clause = |body: Option<anyhow::Result<&LogicalExpr>>,
		                  role: FieldRole,
		                  into: fn(&mut Groups) -> &mut Vec<SchemaRegion>|
		 -> Option<Option<FieldClause>> {
			let Some(body) = body else {
				return Some(None);
			};
			let body = body.ok()?;
			let region = clause_region(field, RegionKind::FieldFrame(role), body, true)?;
			into(&mut groups).push(region);
			Some(Some(FieldClause {
				caller_bound: needs_caller(body),
			}))
		};
		let default = clause(default, FieldRole::Default, |groups| &mut groups.default)?;
		let value = clause(
			field.value_body.as_ref().map(|body| body.logical()),
			FieldRole::Value,
			|groups| &mut groups.value,
		)?;
		let assert = clause(
			field.assert_body.as_ref().map(|body| body.logical()),
			FieldRole::Assert,
			|groups| &mut groups.assert,
		)?;
		let create = field_gate(fields, field, &field.create_permission, registry, &mut groups)?;
		let update = field_gate(fields, field, &field.update_permission, registry, &mut groups)?;
		steps.push(FieldStep::Field(FieldClauses {
			index: u16::try_from(index).ok()?,
			default,
			value,
			assert,
			create,
			update,
		}));
	}
	let Groups {
		default,
		value,
		assert,
		permission,
		derive,
		predicate_derive,
	} = groups;
	let regions = [default, value, assert, permission, derive, predicate_derive]
		.into_iter()
		.flatten()
		.collect();
	Some((steps.into(), regions))
}

/// Field `field`'s `PERMISSIONS FOR create` or `update` clause `permission`, as the stage judges
/// it: a predicate with the computed fields it reads. The predicate is judged in the frame of a
/// permission that a write triggers, under the field's `AUTH LIMIT`. The fields it derives are
/// derived in the gate's derived frame.
fn field_gate(
	fields: &[FieldDefinition],
	field: &FieldDefinition,
	permission: &Permission,
	registry: &FunctionRegistry,
	groups: &mut Groups,
) -> Option<FieldGate> {
	match permission {
		Permission::Full => Some(FieldGate::Allow),
		Permission::None => Some(FieldGate::Deny),
		Permission::Specific(_) => {
			let LogicalPermission::Specific(guard) =
				crate::catalog::LogicalPermissionBody::of(permission).logical().ok()?
			else {
				return None;
			};
			if crate::exec::logical::unplannable_write_in_expr(&guard).is_some() {
				return None;
			}
			let derives = field_guard_derivations(fields, &guard, registry)?;
			let auth = narrowing_auth_limit(&field.auth_limit).ok()?;
			groups.permission.push(SchemaRegion {
				kind: RegionKind::PermissionFrame,
				frame: EvalFrame::mutable_permission(auth),
				body: guard.clone(),
				place: true,
			});
			let mut fields = Vec::with_capacity(derives.len());
			for derivation in derives {
				groups.predicate_derive.push(SchemaRegion {
					kind: RegionKind::FieldFrame(FieldRole::Computed),
					frame: EvalFrame::permission(derivation.auth),
					body: derivation.body,
					place: false,
				});
				fields.push(derivation.field);
			}
			Some(FieldGate::Predicate {
				caller_bound: needs_caller(&guard),
				derives: fields.into(),
			})
		}
	}
}

/// The `COMPUTED` fields of `fields` that the logical permission predicate `predicate` of a field
/// reads, with their bodies, in the order the table's view derives them. If the predicate reads the
/// record other than by name, every computed field is included. Otherwise only the fields it reads
/// by name are included. `None` when a computed field has no logical form or is not named by
/// exactly one field.
fn field_guard_derivations(
	fields: &[FieldDefinition],
	predicate: &LogicalExpr,
	registry: &FunctionRegistry,
) -> Option<Vec<Derivation>> {
	use crate::exec::logical::reads::{reads_beneath, reads_row_opaquely};
	let computed: Vec<&str> = fields
		.iter()
		.filter(|field| field.computed.is_some())
		.map(|field| field.name.field_name())
		.collect::<Option<_>>()?;
	if computed.is_empty() {
		return Some(Vec::new());
	}
	if reads_row_opaquely(predicate, registry) {
		return derivations_of(fields, None);
	}
	let reads: Vec<String> = computed
		.into_iter()
		.filter(|name| {
			reads_beneath(predicate, &[crate::expr::logical::FieldPath::field(name.to_owned())])
		})
		.map(str::to_owned)
		.collect();
	if reads.is_empty() {
		return Some(Vec::new());
	}
	derivations_of(fields, Some(&reads))
}

/// What a write's table declares for the stage that maintains its indexes: the facts the write
/// states, and the regions `StoreIndexData` hosts in the order [`WriteIndexes`] lists them.
#[derive(Default)]
pub(crate) struct TableIndexes {
	pub(crate) facts: WriteIndexes,
	pub(crate) regions: Vec<SchemaRegion>,
}

/// The indexes of `table`, from the catalog's answers at the version the request is bound to.
///
/// A write that names no one table, a table the catalog does not define, an answer the catalog
/// cannot give and a table that declares no index state nothing: the stage that maintains the
/// indexes reads the write's [`Stored`] facts first.
pub(crate) fn indexes_of(catalog: &dyn SemanticCatalog, table: Option<&TableName>) -> TableIndexes {
	let Some(table) = table else {
		return TableIndexes::default();
	};
	let CatalogAnswer::Found(snapshot) = catalog.table(table) else {
		return TableIndexes::default();
	};
	if snapshot.definition.is_none() {
		return TableIndexes::default();
	}
	indexes_over(&snapshot.indexes, snapshot.key.as_deref())
}

/// The indexes `indexes` of a table whose record ids are made of `key`: see [`indexes_of`].
///
/// A column is bound the way an index evaluates it. A read of a component of the record id is bound
/// as a read of the element of the image's own id
/// ([`canonical_columns`](crate::exec::index::derived_columns::canonical_columns)). The guard of a
/// `COUNT` index is bound the same way
/// ([`canonical_guard`](crate::exec::index::derived_columns::canonical_guard)). A column or guard
/// with no logical form, or one that holds a write no operator runs, states no region
/// ([`WriteIndexes::unlowerable`]).
pub(crate) fn indexes_over(
	indexes: &std::sync::Arc<[IndexDefinition]>,
	key: Option<&KeyStructure>,
) -> TableIndexes {
	use crate::exec::index::derived_columns::{canonical_columns, canonical_guard};

	if indexes.is_empty() {
		return TableIndexes::default();
	}
	let facts = WriteIndexes {
		defs: IndexDefs::new(std::sync::Arc::clone(indexes)),
		unlowerable: false,
	};
	let region = |body: LogicalExpr| SchemaRegion {
		kind: RegionKind::Clause(ClauseRole::IndexColumn),
		frame: EvalFrame::caller(),
		body,
		place: false,
	};
	let body_of = |expr: Expr| -> Option<LogicalExpr> {
		let body = crate::expr::logical::lower_expr(expr).ok()?;
		match crate::exec::logical::unplannable_write_in_expr(&body) {
			Some(_) => None,
			None => Some(body),
		}
	};
	let mut regions = Vec::with_capacity(facts.region_count());
	for index in indexes.iter().filter(|index| !index.prepare_remove) {
		for column in canonical_columns(key, &index.cols).iter() {
			let Some(body) = body_of(Expr::Idiom(column.clone())) else {
				return unlowerable(facts);
			};
			regions.push(region(body));
		}
		if let Some(guard) = &index.count_cond {
			let Some(body) = body_of(guard.0.clone()) else {
				return unlowerable(facts);
			};
			let body = canonical_guard(key, &body).unwrap_or(body);
			regions.push(region(body));
		}
	}
	TableIndexes {
		facts,
		regions,
	}
}

/// The body of a clause region the stage that maintains a table's views hosts.
pub(crate) enum ViewBody {
	/// An expression, evaluated over the image the region receives.
	Expr(LogicalExpr),
	/// The projection of the fields of a materialized view over the image the region receives.
	Fields(LogicalFields),
}

/// What a write's table declares for the stage that maintains the views it feeds. It holds the
/// facts the write states, and the regions `ProcessTableViews` hosts, in the order [`WriteViews`]
/// lists them.
#[derive(Default)]
pub(crate) struct TableViews {
	pub(crate) facts: WriteViews,
	pub(crate) regions: Vec<ViewBody>,
}

/// The views the table `table` feeds, from the catalog's answers at the version the request is
/// bound to.
///
/// A write that names no one table, a table the catalog does not define, an answer the catalog
/// cannot give and a table that feeds no view state nothing: the stage that maintains the views
/// reads the write's [`Stored`] facts first.
pub(crate) fn views_of(catalog: &dyn SemanticCatalog, table: Option<&TableName>) -> TableViews {
	let Some(table) = table else {
		return TableViews::default();
	};
	match catalog.views(table) {
		CatalogAnswer::Found(views) if !views.is_empty() => views_over(&views),
		CatalogAnswer::Found(_)
		| CatalogAnswer::Absent
		| CatalogAnswer::NotLoaded
		| CatalogAnswer::Unknown => TableViews::default(),
	}
}

/// The views `views`, the definitions of the view tables a table feeds: see [`views_of`].
///
/// A clause is bound the way the view's maintenance evaluates it, in the frame of a view:
/// permissions off, writes allowed, and no `AUTH LIMIT` narrowing the writer. A clause with no
/// logical form, a clause that holds a write no operator runs, an aggregated view that selects a
/// bare `VALUE`, and a view table with no view state states no region
/// ([`WriteViews::unlowerable`]).
pub(crate) fn views_over(views: &std::sync::Arc<[TableDefinition]>) -> TableViews {
	let facts = WriteViews {
		defs: TableDefs::new(std::sync::Arc::clone(views)),
		unlowerable: false,
	};
	let mut regions = Vec::with_capacity(facts.region_count());
	for definition in views.iter() {
		if view_bodies(definition, &mut regions).is_none() {
			return TableViews {
				facts: WriteViews {
					unlowerable: true,
					..facts
				},
				regions: Vec::new(),
			};
		}
	}
	TableViews {
		facts,
		regions,
	}
}

/// Pushes the clause bodies of the view table `definition` onto `out`, in the order
/// [`WriteViews`] lists them. `None` when a clause has no body to host.
fn view_bodies(definition: &TableDefinition, out: &mut Vec<ViewBody>) -> Option<()> {
	let hosted = |body: LogicalExpr| -> Option<ViewBody> {
		match crate::exec::logical::unplannable_write_in_expr(&body) {
			Some(_) => None,
			None => Some(ViewBody::Expr(body)),
		}
	};
	let of_expr = |expr: &Expr| -> Option<ViewBody> {
		hosted(crate::expr::logical::lower_expr(expr.clone()).ok()?)
	};
	let view = definition.view.as_ref()?;
	match view {
		ViewDefinition::Materialized {
			fields,
			condition,
			..
		} => {
			if condition.is_some() {
				let logical = view.materialized_condition_logical()?.ok()?;
				out.push(hosted(logical.clone())?);
			}
			out.push(ViewBody::Fields(crate::expr::logical::lower_fields(fields.clone()).ok()?));
		}
		ViewDefinition::Aggregated {
			analysis,
			condition,
			..
		} => {
			if let Some(expr) = condition {
				out.push(of_expr(expr)?);
			}
			for expr in analysis.group_expressions.iter().chain(&analysis.aggregate_arguments) {
				out.push(of_expr(expr)?);
			}
			let AggregateFields::Fields(items) = &analysis.fields else {
				return None;
			};
			for (name, expr) in items {
				out.push(of_expr(expr)?);
				for part in &name.0 {
					if let Part::Value(embedded) | Part::Where(embedded) = part {
						out.push(of_expr(embedded)?);
					}
				}
			}
		}
		ViewDefinition::Select {
			..
		} => {}
	}
	Some(())
}

/// What a write's table declares for the stage that purges a deleted record. It holds the facts the
/// write states, and the `THEN` expressions `PurgeRecordData` hosts as regions, in the order
/// [`WriteRepairs`] lists them.
pub(crate) struct TableRepairs {
	/// The write's own `referenced` fact.
	pub(crate) referenced: Stored,
	pub(crate) facts: WriteRepairs,
	pub(crate) regions: Vec<LogicalExpr>,
}

impl Default for TableRepairs {
	fn default() -> Self {
		Self {
			referenced: Stored::Nothing,
			facts: WriteRepairs::default(),
			regions: Vec::new(),
		}
	}
}

/// The repairs a delete of a record of the table `table` makes, from the catalog's answers at the
/// version the request is bound to.
///
/// A write that names no one table states nothing ([`Stored::Dynamic`]). A table the catalog does
/// not define is referenced by no field. An answer the catalog cannot give (it changed before the
/// statement, or the references are not loaded) states [`Stored::LateBound`]. A write whose order
/// does not purge a record (`purges` is `false`) states no repair and does not ask the catalog,
/// because the references of a table are a database-wide read.
pub(crate) fn repairs_of(
	catalog: &dyn SemanticCatalog,
	table: Option<&TableName>,
	purges: bool,
) -> TableRepairs {
	let Some(table) = table else {
		return TableRepairs {
			referenced: Stored::Dynamic,
			..TableRepairs::default()
		};
	};
	if !purges {
		return TableRepairs::default();
	}
	let late = || TableRepairs {
		referenced: Stored::LateBound,
		..TableRepairs::default()
	};
	match catalog.table(table) {
		CatalogAnswer::Found(snapshot) if snapshot.definition.is_some() => {}
		CatalogAnswer::Found(_) | CatalogAnswer::Absent => return TableRepairs::default(),
		CatalogAnswer::NotLoaded | CatalogAnswer::Unknown => return late(),
	}
	match catalog.referencing(table) {
		CatalogAnswer::Found(referencing) => repairs_over(&referencing),
		CatalogAnswer::Absent => TableRepairs::default(),
		CatalogAnswer::NotLoaded | CatalogAnswer::Unknown => late(),
	}
}

/// The repairs of the `REFERENCE` fields `referencing`, grouped by the table that defines them. See
/// [`repairs_of`].
///
/// A `THEN` expression is bound the way the purge evaluates it, in the frame of a view: permissions
/// off, writes allowed, and no `AUTH LIMIT` narrowing the writer. An expression with no logical
/// form, or one that holds a write no operator runs, states no region
/// ([`WriteRepairs::unlowerable`]).
pub(crate) fn repairs_over(referencing: &Referencing) -> TableRepairs {
	let mut tables = Vec::new();
	let mut regions = Vec::new();
	let mut unlowerable = false;
	for (table, fields) in referencing.iter() {
		let mut declared = Vec::new();
		for field in fields.iter() {
			let Some(reference) = &field.reference else {
				continue;
			};
			let strategy = match &reference.on_delete {
				ReferenceDeleteStrategy::Ignore => RepairKind::Ignore,
				ReferenceDeleteStrategy::Reject => RepairKind::Reject,
				ReferenceDeleteStrategy::Cascade => RepairKind::Cascade,
				ReferenceDeleteStrategy::Unset => RepairKind::Unset,
				ReferenceDeleteStrategy::Custom(expr) => {
					match crate::expr::logical::lower_expr(expr.clone()) {
						Ok(body)
							if crate::exec::logical::unplannable_write_in_expr(&body).is_none() =>
						{
							regions.push(body);
						}
						Ok(_) | Err(_) => unlowerable = true,
					}
					RepairKind::Custom
				}
			};
			declared.push(RepairField {
				name: field.name.clone(),
				strategy,
			});
		}
		if !declared.is_empty() {
			tables.push(RepairTable {
				table: table.clone(),
				fields: declared.into_boxed_slice(),
			});
		}
	}
	if tables.is_empty() {
		return TableRepairs::default();
	}
	if unlowerable {
		regions.clear();
	}
	TableRepairs {
		referenced: Stored::Declared,
		facts: WriteRepairs {
			tables: tables.into_boxed_slice(),
			unlowerable,
		},
		regions,
	}
}

/// What the events of a table state for one action: the attributes of the `logical.events` op and
/// the clause regions it hosts, in the order [`EventsAttrs::region_kinds`] lists them.
pub(crate) struct TableEvents {
	pub(crate) attrs: EventsAttrs,
	pub(crate) regions: Vec<SchemaRegion>,
}

/// The events `events` of a table, as they fire for `action`.
///
/// Each event hosts its `WHEN` clause, then each of its `THEN` clauses. An `ASYNC` event states
/// none of them: its clauses run later, in the drain, from the stored definition. A clause runs
/// with permissions off, writes allowed, and under the event's `AUTH LIMIT`. It is evaluated over
/// the image the event is about. The depth a clause spends, and whether its body holds a statement,
/// a closure or a call of a user-defined function, come from the stored clause, as the events' run
/// reads them. A clause with no logical form, a clause that holds a write no operator runs, and an
/// `AUTH LIMIT` that does not convert state no region and mark the op [`EventsAttrs::unlowerable`].
pub(crate) fn events_over(
	events: &std::sync::Arc<[EventDefinition]>,
	action: Action,
) -> TableEvents {
	use crate::exec::vm::depth::ExprDepth;

	let defs = EventDefs::new(std::sync::Arc::clone(events));
	let refused = |defs: EventDefs| TableEvents {
		attrs: EventsAttrs {
			action,
			defs,
			events: Box::default(),
			unlowerable: true,
		},
		regions: Vec::new(),
	};
	let mut shapes = Vec::with_capacity(events.len());
	let mut regions = Vec::new();
	for event in events.iter() {
		let Ok(auth) = narrowing_auth_limit(&event.auth_limit) else {
			return refused(defs);
		};
		let mut clause =
			|source: &Expr, body: anyhow::Result<&LogicalExpr>| -> Option<EventClause> {
				let body = body.ok()?;
				if crate::exec::logical::unplannable_write_in_expr(body).is_some() {
					return None;
				}
				let depth = ExprDepth::of_expr(source);
				let clamp = |n: u32| u8::try_from(n).unwrap_or(u8::MAX);
				regions.push(SchemaRegion {
					kind: RegionKind::EventFrame,
					frame: EvalFrame::mutable_permission(auth.clone()),
					body: body.clone(),
					place: false,
				});
				Some(EventClause {
					deepest: clamp(depth.deepest),
					write: clamp(depth.charged()),
					caller_bound: needs_caller(body),
				})
			};
		let Some(when) = clause(&event.when, event.when_logical()) else {
			return refused(defs);
		};
		let mut then = Vec::new();
		if !event.is_async() {
			for (source, body) in event.then.iter().zip(event.then_logical()) {
				let Some(shape) = clause(source, body) else {
					return refused(defs);
				};
				then.push(shape);
			}
		}
		shapes.push(EventShape {
			when,
			then: then.into_boxed_slice(),
		});
	}
	TableEvents {
		attrs: EventsAttrs {
			action,
			defs,
			events: shapes.into_boxed_slice(),
			unlowerable: false,
		},
		regions,
	}
}

/// The indexes `facts` states, with no region: some column or guard cannot be lowered.
fn unlowerable(facts: WriteIndexes) -> TableIndexes {
	TableIndexes {
		facts: WriteIndexes {
			unlowerable: true,
			..facts
		},
		regions: Vec::new(),
	}
}

/// Why the field schema `stored` and the image facts `image` do not describe a write whose stage
/// ops are those of `write`, or `None` when they do.
///
/// `ProcessTableFields` must host exactly the regions that the schema's steps state, of the kinds
/// and in the order [`WriteSchema::region_counts`] lists. `GenerateRecordId` must host the
/// `DEFAULT` of the `id` field exactly when the image states one. A write whose first operand is
/// not the result of a write loop states nothing to check.
pub(crate) fn schema_fault(
	ir: &Ir,
	write: &OpData,
	stored: &WriteStored,
	image: WriteImage,
) -> Option<&'static str> {
	let ops = write_stage_ops(ir, write)?;
	let kinds_of = |op: OpId| -> Option<Vec<RegionKind>> {
		let regions = &ir.op(op)?.regions;
		Some(
			regions
				.iter()
				.skip(1)
				.filter_map(|region| ir.region(*region).map(|held| held.kind))
				.collect(),
		)
	};
	let schema = match stored.fields {
		Stored::Declared => stored.schema.region_counts().kinds(),
		Stored::Nothing | Stored::Dynamic | Stored::LateBound => Vec::new(),
	};
	let id = if image.id_default && !stored.schema.unlowerable {
		vec![RegionKind::FieldFrame(FieldRole::Default)]
	} else {
		Vec::new()
	};
	let indexes = match stored.indexes {
		Stored::Declared => stored.index_set.region_count(),
		Stored::Nothing | Stored::Dynamic | Stored::LateBound => 0,
	};
	let views = match stored.views {
		Stored::Declared => stored.view_set.region_count(),
		Stored::Nothing | Stored::Dynamic | Stored::LateBound => 0,
	};
	let repairs = match stored.referenced {
		Stored::Declared => stored.repair_set.region_count(),
		Stored::Nothing | Stored::Dynamic | Stored::LateBound => 0,
	};
	for (op, stage) in ops {
		let held = kinds_of(op)?;
		match stage {
			WriteStageKind::ProcessTableFields if held != schema => {
				return Some("the field clauses of a write are not the regions its schema states");
			}
			WriteStageKind::GenerateRecordId if held != id => {
				return Some("the id DEFAULT of a write is not the region its image states");
			}
			WriteStageKind::StoreIndexData
				if held.len() != indexes
					|| held
						.iter()
						.any(|kind| *kind != RegionKind::Clause(ClauseRole::IndexColumn)) =>
			{
				return Some("the index columns of a write are not the regions its indexes state");
			}
			WriteStageKind::ProcessTableViews(_)
				if held.len() != views
					|| held
						.iter()
						.any(|kind| *kind != RegionKind::Clause(ClauseRole::ViewClause)) =>
			{
				return Some("the view clauses of a write are not the regions its views state");
			}
			WriteStageKind::PurgeRecordData
				if held.len() != repairs
					|| held
						.iter()
						.any(|kind| *kind != RegionKind::Clause(ClauseRole::ReferenceRepair)) =>
			{
				return Some(
					"the repair clauses of a write are not the regions its references state",
				);
			}
			WriteStageKind::ProcessTableFields
			| WriteStageKind::GenerateRecordId
			| WriteStageKind::CheckRecordUserAccess
			| WriteStageKind::CheckRecordExists
			| WriteStageKind::CheckTableNotView
			| WriteStageKind::CheckTableNotLightweight(_)
			| WriteStageKind::CheckTableType(_)
			| WriteStageKind::CheckPermissionsQuickCreate
			| WriteStageKind::CheckUpdatePermissions
			| WriteStageKind::CheckDeletePermissions
			| WriteStageKind::CheckCreatePermissions
			| WriteStageKind::RecheckUpdatePermissions
			| WriteStageKind::CheckWhereCondition
			| WriteStageKind::ComputeInputData
			| WriteStageKind::CheckDataFields
			| WriteStageKind::ProcessRecordData
			| WriteStageKind::ProcessMergeData
			| WriteStageKind::DefaultRecordData
			| WriteStageKind::CleanupTableFields
			| WriteStageKind::CleanupTableReferences
			| WriteStageKind::ClearRecordData
			| WriteStageKind::StoreRecordData
			| WriteStageKind::StoreEdgesData
			| WriteStageKind::StoreInlineAdjacencyData
			| WriteStageKind::StoreIndexData
			| WriteStageKind::ReclaimDocId
			| WriteStageKind::PurgeRecordData
			| WriteStageKind::MaterialiseObservedFields
			| WriteStageKind::ProcessTableReferences
			| WriteStageKind::ProcessTableViews(_)
			| WriteStageKind::ProcessTableEvents(_)
			| WriteStageKind::ProcessTableLives(_)
			| WriteStageKind::ProcessChangefeeds
			| WriteStageKind::CheckSelectPermissions(_)
			| WriteStageKind::OutputWrite => {}
		}
	}
	None
}

/// The stage ops of the write `write`, each with whether it hosts a `PermissionFrame` region, or
/// `None` when the write's first operand is not the result of a write loop.
pub(crate) fn write_gate_hosts(ir: &Ir, write: &OpData) -> Option<Vec<(WriteStageKind, bool)>> {
	let ops = write_stage_ops(ir, write)?;
	Some(
		ops.into_iter()
			.map(|(op, stage)| {
				let hosts = ir.op(op).is_some_and(|data| {
					data.regions
						.iter()
						.filter_map(|region| ir.region(*region))
						.any(|region| region.kind == RegionKind::PermissionFrame)
				});
				(stage, hosts)
			})
			.collect(),
	)
}

/// Why the gate clauses `gates` do not describe a write whose stage ops are `stages`, each with
/// whether it hosts a predicate region, or `None` when they do.
///
/// A gate stage hosts a predicate exactly when its clause is one, and no other stage hosts one
/// but `ProcessTableFields`, whose field permission predicates are regions of its schema, which
/// [`schema_fault`] checks.
pub(crate) fn gates_fault(
	gates: WriteGates,
	stages: &[(WriteStageKind, bool)],
) -> Option<&'static str> {
	for (stage, hosts) in stages {
		let clause = match stage {
			WriteStageKind::CheckCreatePermissions => Some(gates.create),
			WriteStageKind::CheckUpdatePermissions | WriteStageKind::RecheckUpdatePermissions => {
				Some(gates.update)
			}
			WriteStageKind::CheckDeletePermissions => Some(gates.delete),
			WriteStageKind::CheckSelectPermissions(_) => Some(gates.select),
			WriteStageKind::ProcessTableFields => continue,
			WriteStageKind::CheckRecordUserAccess
			| WriteStageKind::CheckRecordExists
			| WriteStageKind::CheckTableNotView
			| WriteStageKind::CheckTableNotLightweight(_)
			| WriteStageKind::CheckTableType(_)
			| WriteStageKind::CheckPermissionsQuickCreate
			| WriteStageKind::CheckWhereCondition
			| WriteStageKind::ComputeInputData
			| WriteStageKind::CheckDataFields
			| WriteStageKind::ProcessRecordData
			| WriteStageKind::ProcessMergeData
			| WriteStageKind::DefaultRecordData
			| WriteStageKind::GenerateRecordId
			| WriteStageKind::CleanupTableFields
			| WriteStageKind::CleanupTableReferences
			| WriteStageKind::ClearRecordData
			| WriteStageKind::StoreRecordData
			| WriteStageKind::StoreEdgesData
			| WriteStageKind::StoreInlineAdjacencyData
			| WriteStageKind::StoreIndexData
			| WriteStageKind::ReclaimDocId
			| WriteStageKind::PurgeRecordData
			| WriteStageKind::MaterialiseObservedFields
			| WriteStageKind::ProcessTableReferences
			| WriteStageKind::ProcessTableViews(_)
			| WriteStageKind::ProcessTableEvents(_)
			| WriteStageKind::ProcessTableLives(_)
			| WriteStageKind::ProcessChangefeeds
			| WriteStageKind::OutputWrite => None,
		};
		let predicate = matches!(clause, Some(GateClause::Predicate { .. }));
		if *hosts != predicate {
			return Some(if *hosts {
				"a stage hosts a permission predicate its clause does not state"
			} else {
				"a gate stage whose clause is a predicate hosts none"
			});
		}
	}
	None
}

/// The definition of the view table `name`, as `DEFINE TABLE name AS SELECT fields FROM t [WHERE
/// cond] [GROUP BY groups]` forms it.
#[cfg(test)]
pub(crate) fn view_table_for_test(
	name: &str,
	fields: &str,
	cond: Option<&str>,
	groups: Option<&[&str]>,
) -> TableDefinition {
	use crate::catalog::{
		DatabaseId, FromStored, NamespaceId, StoredTableDefinition, TableId, view_to_definition,
	};
	use crate::expr::{Cond, Group, Groups, View};

	let view = View {
		expr: crate::syn::runtime::fields_for_definition(fields).expect("fields parse"),
		what: vec![TableName::new("t")],
		cond: cond.map(|text| Cond(crate::syn::expr(text).expect("a condition"))),
		group: groups.map(|groups| {
			Groups(
				groups
					.iter()
					.map(|g| Group(g.parse::<crate::expr::Idiom>().expect("an idiom")))
					.collect(),
			)
		}),
	};
	let mut definition = TableDefinition::from_stored(&StoredTableDefinition::new(
		NamespaceId(0),
		DatabaseId(0),
		TableId(0),
		name.into(),
	))
	.expect("a table definition");
	definition.view = Some(view_to_definition(&view).expect("a view"));
	definition
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Every combination of the facts a statement contributes to the shape of its orders.
	fn every_facts() -> Vec<(PlanForm, WriteFacts)> {
		let mut all = Vec::new();
		for form in [
			PlanForm::Create,
			PlanForm::Update,
			PlanForm::Upsert,
			PlanForm::Delete,
			PlanForm::Insert,
			PlanForm::Relate,
		] {
			for bits in 0..16_u8 {
				all.push((
					form,
					WriteFacts {
						form,
						has_data: bits & 1 != 0,
						on_duplicate_key_update: bits & 2 != 0,
						ignore: bits & 4 != 0,
						repeatable: bits & 8 != 0,
					},
				));
			}
		}
		all
	}

	#[test]
	fn the_body_of_every_form_names_every_stage_of_its_run() {
		for (form, facts) in every_facts() {
			let run = stage_run(form, facts);
			for cond in [false, true] {
				let body = match write_body(form, facts, &run, cond) {
					Ok(body) => body,
					Err(error) => panic!("{form:?} {facts:?} has a body: {error}"),
				};
				assert_eq!(body_fault(&body, run.len()), None, "{form:?} {facts:?}");
			}
		}
	}

	#[test]
	fn a_body_names_the_stages_of_each_order_in_the_run() {
		for (form, facts) in every_facts() {
			let run = stage_run(form, facts);
			let body = write_body(form, facts, &run, false).expect("the body derives");
			let held = orders(form, facts);
			let named = |positions: &[u16]| -> Vec<WriteStageKind> {
				positions.iter().map(|at| run[usize::from(*at)]).collect()
			};
			assert_eq!(named(&body.first), held[0].1.to_vec(), "{form:?} {facts:?}");
			match body.branching {
				WriteBranching::Sole | WriteBranching::ByAction => {
					assert!(body.second.is_empty())
				}
				WriteBranching::UpFront | WriteBranching::Retry(_) => {
					assert_eq!(named(&body.second), held[1].1.to_vec(), "{form:?} {facts:?}");
				}
			}
		}
	}

	#[test]
	fn a_form_branches_as_the_executor_does() {
		let facts = |form| WriteFacts {
			form,
			has_data: true,
			on_duplicate_key_update: false,
			ignore: false,
			repeatable: false,
		};
		let branching = |form| {
			let facts = facts(form);
			let run = stage_run(form, facts);
			write_body(form, facts, &run, false).expect("the body derives").branching
		};
		assert_eq!(branching(PlanForm::Create), WriteBranching::Sole);
		assert_eq!(branching(PlanForm::Update), WriteBranching::Sole);
		assert_eq!(branching(PlanForm::Delete), WriteBranching::Sole);
		assert_eq!(branching(PlanForm::Relate), WriteBranching::UpFront);
		assert!(matches!(branching(PlanForm::Upsert), WriteBranching::Retry(_)));
		assert!(matches!(branching(PlanForm::Insert), WriteBranching::Retry(_)));
	}

	#[test]
	fn a_table_names_a_record_to_create_for_create_and_an_unconditional_upsert() {
		let facts = |form| WriteFacts {
			form,
			has_data: true,
			on_duplicate_key_update: false,
			ignore: false,
			repeatable: false,
		};
		let deferable = |form, cond| {
			let facts = facts(form);
			let run = stage_run(form, facts);
			write_body(form, facts, &run, cond).expect("the body derives").deferable
		};
		assert!(deferable(PlanForm::Create, false));
		assert!(deferable(PlanForm::Create, true));
		assert!(deferable(PlanForm::Upsert, false));
		assert!(!deferable(PlanForm::Upsert, true));
		assert!(!deferable(PlanForm::Update, false));
		assert!(!deferable(PlanForm::Insert, false));
	}

	#[test]
	fn a_view_row_has_no_body() {
		let facts = WriteFacts {
			form: PlanForm::View,
			has_data: false,
			on_duplicate_key_update: false,
			ignore: false,
			repeatable: false,
		};
		assert!(write_body(PlanForm::View, facts, &[], false).is_err());
	}

	/// A view row's run keeps the order of the stage kinds, its body names every stage of it, and
	/// the order of each action is the stages of the view-row order the write plan holds for it.
	#[test]
	fn a_view_row_runs_the_order_of_the_action_its_maintenance_settled() {
		let facts = WriteFacts {
			form: PlanForm::View,
			has_data: false,
			on_duplicate_key_update: false,
			ignore: false,
			repeatable: false,
		};
		let run = stage_run(PlanForm::View, facts);
		assert_eq!(
			run,
			vec![
				WriteStageKind::StoreIndexData,
				WriteStageKind::ProcessTableViews(Action::Create),
				WriteStageKind::ProcessTableViews(Action::Update),
				WriteStageKind::ProcessTableViews(Action::Delete),
				WriteStageKind::ProcessTableEvents(Action::Create),
				WriteStageKind::ProcessTableEvents(Action::Update),
				WriteStageKind::ProcessTableEvents(Action::Delete),
				WriteStageKind::ProcessTableLives(Action::Create),
				WriteStageKind::ProcessTableLives(Action::Update),
				WriteStageKind::ProcessTableLives(Action::Delete),
				WriteStageKind::ProcessChangefeeds,
			]
		);
		let body = write_body(PlanForm::View, facts, &run, false).expect("the body derives");
		assert_eq!(body.branching, WriteBranching::ByAction);
		assert_eq!(body_fault(&body, run.len()), None);
		let held = orders(PlanForm::View, facts);
		for ((_, stages), action) in
			held.iter().zip([Action::Create, Action::Update, Action::Delete])
		{
			let order = view_row_order(&body, &run, action).expect("the order derives");
			assert_eq!(order, stages.to_vec(), "{action:?}");
		}
		assert!(view_row_order(&sole(&[0], &[]), &run, Action::Create).is_err());
	}

	fn sole(first: &[u16], second: &[u16]) -> WriteBody {
		WriteBody {
			branching: WriteBranching::Sole,
			first: first.into(),
			second: second.into(),
			deferable: false,
		}
	}

	#[test]
	fn a_body_that_breaks_a_rule_is_refused() {
		let retry = WriteRetry {
			savepoint_after: 1,
			retry_conflict: true,
			skip_failures: false,
			carry_schema_failure: false,
		};
		let two = |branching, first: &[u16], second: &[u16]| WriteBody {
			branching,
			first: first.into(),
			second: second.into(),
			deferable: false,
		};
		// Positions that repeat or fall beyond the stages, and a first order that does not ascend.
		assert!(body_fault(&sole(&[1, 0, 2], &[]), 3).is_some());
		assert!(body_fault(&sole(&[0, 0, 1, 2], &[]), 3).is_some());
		assert!(body_fault(&two(WriteBranching::UpFront, &[0, 1, 2], &[0, 2, 2]), 3).is_some());
		assert!(body_fault(&sole(&[0, 1, 3], &[]), 3).is_some());
		// An empty first order.
		assert!(body_fault(&sole(&[], &[]), 0).is_some());
		// A form with one order that names a second, and a form with two that names none.
		assert!(body_fault(&sole(&[0, 1], &[1]), 2).is_some());
		assert!(body_fault(&two(WriteBranching::UpFront, &[0, 1], &[]), 2).is_some());
		assert!(body_fault(&two(WriteBranching::Retry(retry), &[0, 1], &[]), 2).is_some());
		// A savepoint beyond the creating order.
		let beyond = WriteRetry {
			savepoint_after: 3,
			..retry
		};
		assert!(body_fault(&two(WriteBranching::Retry(beyond), &[0, 1], &[0]), 2).is_some());
		// A stage that no order runs.
		assert!(body_fault(&sole(&[0, 1], &[]), 3).is_some());
		assert_eq!(body_fault(&two(WriteBranching::Retry(retry), &[0, 1], &[0, 2]), 3), None);
		// An updating order that runs shared stages in another relative order than the creating
		// one.
		assert_eq!(body_fault(&two(WriteBranching::UpFront, &[0, 1, 2], &[0, 2, 1]), 3), None);
	}

	/// A clause that reads a computed field derives it, and every computed field it reads in turn,
	/// and nothing else.
	#[test]
	fn the_fields_a_clause_derives_are_the_computed_fields_it_reaches() {
		let order = |entries: &[(&str, &[&str])]| -> Vec<(String, Vec<String>)> {
			entries
				.iter()
				.map(|(name, reads)| {
					((*name).to_owned(), reads.iter().map(|read| (*read).to_owned()).collect())
				})
				.collect()
		};
		let fields = order(&[("total", &["net", "tax"]), ("tax", &["net"]), ("label", &["name"])]);
		let reached = |reads: &[&str]| {
			let reads: Vec<String> = reads.iter().map(|read| (*read).to_owned()).collect();
			let mut found = needed_fields(&fields, &reads);
			found.sort();
			found
		};
		assert_eq!(reached(&["total"]), ["tax", "total"]);
		assert_eq!(reached(&["tax", "owner"]), ["tax"]);
		assert!(reached(&["owner", "net"]).is_empty());
	}
}
