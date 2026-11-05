//! Lowers the stages of a write's per-record body.
//!
//! A stage lowers to the instructions that the write body runs for it. The instructions jump to
//! the `skip` label when the stage drops the record. The write decides which stages run and in
//! what order ([`WriteBody`](crate::exec::ir::logical::ops::WriteBody)). This module makes no such
//! decision. It reads no catalog. Anything a stage needs from a table's definitions comes from a
//! clause region on its op. If the lowering has no form for a stage's clause, the stage returns
//! [`IrError::NotYet`] with the stage's name.
//!
//! # What a stage reads
//!
//! - The storage stages (`CheckRecordExists` through `StoreInlineAdjacencyData`, and
//!   `ReclaimDocId`) and the observers of the change feed and of live queries emit fixed
//!   instructions. These pick the table from the record's write context at run time.
//! - The data stages read the clauses that the statement places on its stage ops. The condition is
//!   the `WriteCond` region of `CheckWhereCondition`. A data clause is the `SetValue`, `Content` or
//!   `DuplicateUpdate` region of `ComputeInputData`. Each one is a host pipeline over the record's
//!   pre-mutation snapshot. A condition emits its value. A `SET` emits one value per assignment, in
//!   written order. Any other clause emits its one value and the mode it is stored in. The places a
//!   clause writes to belong to the write's own steps, and this module never evaluates them.
//!   `ProcessRecordData` applies the evaluated clause. `DefaultRecordData` and `GenerateRecordId`
//!   set the record's special fields and its id from the write's [`WriteImage`].
//! - The permission gates (`CheckRecordUserAccess`, `CheckPermissionsQuickCreate`, the create,
//!   update and delete checks, `RecheckUpdatePermissions` and `CheckSelectPermissions`) read the
//!   clause that the write gives them ([`WriteGates`]). A `FULL` clause admits the record. A `NONE`
//!   clause, or a table the catalog does not define, refuses every record when the caller's actions
//!   are permission-checked. A predicate is the `PermissionFrame` region of its gate stage op. It
//!   is a host pipeline over a copy of the image that the gate names, bound as both the value and
//!   the document root. It is judged in the gate frame of its guard: a read guard refuses writes,
//!   and what the predicate reads is not permission-checked. The record is skipped when the verdict
//!   is falsy. A predicate that may read a `COMPUTED` field first derives the fields it reads onto
//!   that copy. A second host pipeline does this, with one `FieldFrame(Computed)` region per field
//!   after the predicate, in the order the write lists them ([`WriteDerived`]). Each body runs in
//!   the gate's derived frame: reads are unchecked, writes are refused, and the field's `AUTH
//!   LIMIT` applies. The result is coerced to the field's kind and stored before the next field is
//!   derived. A clause that the write does not state is never treated as `FULL`. A write that names
//!   no single table returns `NotYet`. A clause read before a catalog change returns
//!   [`IrError::LateBind`]. A clause with no logical form is an invariant break.
//! - `OutputWrite` shows the record's images through the written table's field view
//!   ([`ImageView`]). A plain view is no view at all, and neither is the view of a table whose only
//!   cut fields belong to a caller whose reads are not permission-checked. Any other view is the
//!   host pipeline of the layers that the write states ([`ImageLayers`]). It is lowered once from
//!   the regions that the write op hosts after its `RETURN` region, and every stage that shows or
//!   reads an image through the view shares it. A view with no layer form makes the host run an
//!   image through the table's field declarations when it shows it. A view that cuts or derives,
//!   and states neither layers nor declarations, is an invariant break. A `RETURN` of fields is the
//!   `Clause(ReturnClause)` region that the write op hosts. It is a host pipeline over the shown
//!   post-image, and the host binds its `$before` to the shown pre-image. `RETURN *` alone has no
//!   pipeline, because the image itself is the result. The image of a record that a `DELETE`
//!   removes is `NONE`, so a `*` projected over it returns `NotYet`.
//! - The stages that maintain what the table stores (`ProcessTableFields`, `CleanupTableFields`,
//!   `MaterialiseObservedFields`, the reference stages, `StoreIndexData`, `ProcessTableViews` and
//!   `ProcessTableEvents`) read the write's [`WriteStored`]. A table that stores none of a kind
//!   gets a stage that emits what a table with nothing to maintain needs. For an index or an event
//!   that means no instruction, when the statement cannot change the catalog. A write that names no
//!   single table returns `NotYet`, and a definition read before a catalog change returns
//!   [`IrError::LateBind`]. A table with references has them synced by the field names that the
//!   write states. A table with indexes has each index maintained from the values its columns give
//!   for the record's two images. Each index gets one host pipeline, over a `Clause(IndexColumn)`
//!   region per column, in the frame that an index evaluates in. The definitions come from the
//!   write ([`WriteIndexes`](crate::exec::ir::logical::ops::WriteIndexes)). The events of a table
//!   form one unit ([`UnitRequest::Events`]) that runs each event's clauses for the record.
//!   `ProcessTableEvents` asks the compile driver for this unit, with the frame that the write is
//!   lowered for, and the unit runs at the computation depth of the write's place in the request
//!   ([`StageRun::nesting`]). A statement that may define an event asks for the unit even when the
//!   table holds none. A table that feeds views has each view maintained by `ProcessTableViews`.
//!   The view definitions come from the write
//!   ([`WriteViews`](crate::exec::ir::logical::ops::WriteViews)), and the clauses come from the
//!   `Clause(ViewClause)` regions of the stage. Those clauses run in the frame of a view. A
//!   materialized view has a condition and a projection. An aggregated view has a condition, group
//!   expressions, aggregate arguments and fields, and the subscripts and filters that its field
//!   names embed. Each clause is a host pipeline of the program, and it must handle an image that
//!   is an object as well as any other value. The order of each view table's rows
//!   ([`UnitRequest::ViewRows`]) and the recalculation of an aggregated view's extremes
//!   ([`UnitRequest::ViewRecalc`]) are units that the compile driver plans. The views are lowered
//!   once for the write, whichever of its orders maintains them. A stage never concludes that a
//!   table stores nothing from a missing clause region. A table that stores fields has every
//!   `COMPUTED` field derived onto both images of an observed record by
//!   `MaterialiseObservedFields`. That stage hosts one `FieldFrame(Computed)` region per field, in
//!   the computed frame, in the order [`WriteDerived`] gives.
//! - `PurgeRecordData` deletes the record, purges an edge's adjacency keys, and runs the cascades.
//!   The cascades cover the edges joined to the record and each record that references it. A
//!   referencing record is repaired the way its `REFERENCE` field directs
//!   ([`WriteRepairs`](crate::exec::ir::logical::ops::WriteRepairs)). A repair that deletes or
//!   unsets is a statement that the compile driver plans as a unit ([`UnitRequest::Cascade`]). A
//!   `THEN` repair is a host pipeline of the program, lowered from the `Clause(ReferenceRepair)`
//!   region of the stage over the id of the referencing record.
//!
//! Every snapshot and every image that a stage shows is cut for a caller whose reads are
//! permission-checked. Whether that holds is a fact of the frame the program is lowered for
//! ([`StageRun::checked`]). A view that cuts or derives, and states no layers, is refused. It is
//! never shown whole.

#![deny(clippy::wildcard_enum_match_arm)]

use std::sync::Arc;

use surrealdb_strand::{Strand, TableName};
use surrealdb_types::ToSql as _;

use super::super::program::{
	Enforce, FrameMode, Instr, Label, Operand, ParamSpec, PermissionSpec, Pipeline, PlaceStepSpec,
	ROW, Reg, Source, UnitId, WriteInstr, WriteOutput,
};
use super::super::write::{
	DataInstr, FieldPermission, FieldPlan, FieldWrite, GateInstr, IndexPlan, LoweredIndexes,
	LoweredView, LoweredViews, MaterializeStep, NativeIndexKind, ObserveInstr, Recalc,
	ReferenceInstr, RepairStrategy, SchemaInstr, SchemaPlan, StorageFact, StorageInstr,
	TableRepairs, ViewClause, ViewRows,
};
use super::emit::Emitter;
use super::expr::{LowerLimits, Lowerer, Needs, ends_context, invariant};
use super::pipe::{
	first_arg, key_parts, lower_layers, lower_stored_guards, project_image, region_frame,
	yielded_value,
};
use super::read::producer_of;
use super::units::{CascadeKind, LowerCx, UnitRequest};
use super::write_place::raises_before_storing;
use super::write_stmt::StageSink;
use crate::exec::field_path::FieldPath;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::{FrameId, RegionId};
use crate::exec::ir::iface::region_kind::{ClauseRole, FieldRole, RegionKind};
use crate::exec::ir::iface::statements::{ContentMode, OutputShape, WriteForm};
use crate::exec::ir::logical::ops::{
	DerivedField, FieldClause, FieldGate, FieldStep, GateClause, ImageLayers, ImageView, LogOp,
	RepairKind, SchemaRegions, Stored, SubscriberLayer, WriteDerived, WriteGates, WriteImage,
	WriteStageKind, WriteStored,
};
use crate::exec::ir::logical::stages::{view_reads_data, write_stage_ops};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::{PhysOp, ProjectionShape};
use crate::exec::operators::write::stage::RecordImage;
use crate::exec::operators::write::stages::view::extremes_of;
use crate::expr::logical::{EvalFrame, EvalMode, GuardKind};
use crate::expr::{Idiom, Part};
use crate::iam::Action;

/// The stages of one write, in the order the subject loop's body holds them, and the clauses its
/// stage ops host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StageRun {
	/// The stage of each stage op of the loop body.
	pub(super) stages: Vec<WriteStageKind>,
	/// The `WriteCond` region `CheckWhereCondition` hosts, for a statement that has a `WHERE`
	/// condition.
	pub(super) cond: Option<RegionId>,
	/// The data clause `ComputeInputData` hosts, for a statement that has one.
	pub(super) data: Option<DataClause>,
	/// What the statement returns, which `OutputWrite` shows.
	pub(super) output: OutputShape,
	/// The `Clause(ReturnClause)` region the write op itself hosts, for a `RETURN` of fields: the
	/// projection of the record's images.
	pub(super) returns: Option<RegionId>,
	/// The statement's form, which decides whether the target's own permission refuses a record
	/// before it is read.
	pub(super) form: WriteForm,
	/// The permission clauses the gate stages judge.
	pub(super) gates: WriteGates,
	/// The regions each stage hosts for the clause it judges and the fields it derives. Every
	/// stage op of a kind hosts the regions of the one clause the write states for it, so the
	/// first op of a kind that hosts any stands for all of them.
	pub(super) hosted: Vec<(WriteStageKind, Hosted)>,
	/// The `COMPUTED` fields the stages derive, which the `FieldFrame` regions they host evaluate.
	pub(super) derived: WriteDerived,
	/// What the written table's field declarations say about the images of its records.
	pub(super) image: WriteImage,
	/// What the written table stores that the stages of the write run: its fields, references,
	/// indexes, events and views.
	pub(super) stored: WriteStored,
	/// The regions `ProcessTableFields` hosts for the clauses of the table's fields, by kind.
	pub(super) fields: FieldRegions,
	/// The `FieldFrame(Default)` region `GenerateRecordId` hosts, for a table whose `id` field
	/// declares a `DEFAULT`.
	pub(super) id_default: Option<RegionId>,
	/// The `Clause(IndexColumn)` regions `StoreIndexData` hosts for the columns and guards of the
	/// table's indexes, in the order
	/// [`WriteIndexes`](crate::exec::ir::logical::ops::WriteIndexes) lists them.
	pub(super) indexes: Vec<RegionId>,
	/// The `Clause(ViewClause)` regions `ProcessTableViews` hosts for the clauses of the views the
	/// table feeds, in the order [`WriteViews`](crate::exec::ir::logical::ops::WriteViews) lists
	/// them.
	pub(super) views: Vec<RegionId>,
	/// The `Clause(ReferenceRepair)` regions `PurgeRecordData` hosts for the `THEN` expressions of
	/// the references it repairs, in the order
	/// [`WriteRepairs`](crate::exec::ir::logical::ops::WriteRepairs) lists them.
	pub(super) repairs: Vec<RegionId>,
	/// The regions the write op itself hosts for the layers of the table's field view, after its
	/// `RETURN` region: one run for each layer of
	/// [`ImageLayers::Stack`](crate::exec::ir::logical::ops::ImageLayers::Stack), in the order
	/// the layer states them.
	pub(super) layers: Vec<Vec<RegionId>>,
	/// Whether the frame the program is lowered for has its reads permission-checked and does not
	/// suppress the check: the caller whose images a table's cut fields reduce.
	pub(super) checked: bool,
	/// Where the write stands in the container that holds it ([`statement_nesting`]), which,
	/// within where the container stands, is the computation-depth count resumed for the events
	/// a record fires ([`StatementNesting::depth`]).
	pub(super) nesting: StatementNesting,
}

/// The regions the stage op that applies the field schema hosts after its own, by kind and in the
/// order of the steps that state them
/// ([`WriteSchema::region_counts`](crate::exec::ir::logical::ops::WriteSchema::region_counts)).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct FieldRegions {
	/// The `FieldFrame(Default)` regions.
	pub(super) default: Vec<RegionId>,
	/// The `FieldFrame(Value)` regions.
	pub(super) value: Vec<RegionId>,
	/// The `FieldFrame(Assert)` regions.
	pub(super) assert: Vec<RegionId>,
	/// The `PermissionFrame` regions of the predicates among the fields' `create` and `update`
	/// permissions.
	pub(super) permission: Vec<RegionId>,
	/// The `FieldFrame(Computed)` regions of the `Derive` steps.
	pub(super) derive: Vec<RegionId>,
	/// The `FieldFrame(Computed)` regions the predicates derive, in the order of the predicates.
	pub(super) predicate_derive: Vec<RegionId>,
}

impl FieldRegions {
	/// The regions `regions`, the regions of a `ProcessTableFields` op, as the schema's counts
	/// `counts` partition them. Answers [`IrError::Invariant`] when the op hosts other regions
	/// than the schema states: a clause is never read from a region the schema does not state.
	fn of(ir: &Ir, regions: &[RegionId], counts: SchemaRegions) -> Result<Self, IrError> {
		let held = regions
			.iter()
			.skip(1)
			.map(|region| ir.region(*region).map(|data| (*region, data.kind)))
			.collect::<Option<Vec<_>>>()
			.ok_or(IrError::Stale("ir_lower::stage: a region of ProcessTableFields is gone"))?;
		if held.iter().map(|(_, kind)| *kind).ne(counts.kinds()) {
			return Err(IrError::Invariant {
				what: "ir_lower::stage: the field clauses of a write are not the regions its schema states",
			});
		}
		let mut regions = held.into_iter().map(|(region, _)| region);
		let mut take = |count: usize| regions.by_ref().take(count).collect::<Vec<_>>();
		Ok(Self {
			default: take(counts.default),
			value: take(counts.value),
			assert: take(counts.assert),
			permission: take(counts.permission),
			derive: take(counts.derive),
			predicate_derive: take(counts.predicate_derive),
		})
	}
}

/// The regions a stage op hosts beyond the clauses of its statement.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Hosted {
	/// The `PermissionFrame` region of a permission gate whose clause is a predicate.
	pub(super) predicate: Option<RegionId>,
	/// The `FieldFrame(Computed)` regions that derive a computed field each, in the order
	/// [`WriteDerived`] lists the fields.
	pub(super) derivations: Vec<RegionId>,
}

/// The data clause of a statement, as the region of its `ComputeInputData` stage that holds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct DataClause {
	/// The clause region. Its one block argument is the record's snapshot.
	pub(super) region: RegionId,
	/// The clause's role: `SetValue`, `Content`, or `DuplicateUpdate`, whose `$input` is the value
	/// the statement supplied for the record.
	pub(super) role: ClauseRole,
	/// What the clause says.
	pub(super) kind: ClauseKind,
}

/// What a data clause says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ClauseKind {
	/// `SET` or `ON DUPLICATE KEY UPDATE`: one value per assignment, in written order.
	Set,
	/// `UNSET`: no value is evaluated.
	Unset,
	/// Any other clause: the one value it stores, and the mode it stores it in.
	Content(ContentMode),
}

impl StageRun {
	/// The stages of the write `write` and the clauses they host, for a program lowered for a
	/// frame that is `checked` when its reads are permission-checked and its check is not
	/// suppressed. Answers [`IrError::Stale`] for a write whose subjects are not the result of a
	/// write loop, or whose clauses are not the ones a statement binds.
	pub(super) fn of(ir: &Ir, write: &OpData, checked: bool) -> Result<Self, IrError> {
		let ops = write_stage_ops(ir, write)
			.ok_or(IrError::Stale("ir_lower::stage: the subjects of a write are no write loop"))?;
		let (output, form, gates, image, stored, derived) = match &write.kind {
			OpKind::Logical(LogOp::Write(attrs)) => (
				attrs.output,
				attrs.form,
				attrs.gates,
				attrs.image,
				attrs.stored.clone(),
				attrs.derived.clone(),
			),
			OpKind::Phys(PhysOp::Write(attrs)) => (
				attrs.write.output,
				attrs.write.form,
				attrs.write.gates,
				attrs.write.image,
				attrs.write.stored.clone(),
				attrs.write.derived.clone(),
			),
			OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_) => {
				return Err(IrError::Stale("ir_lower::stage: the op of a write is no write"));
			}
		};
		let returns = write.regions.iter().copied().find(|region| {
			ir.region(*region).map(|held| held.kind)
				== Some(RegionKind::Clause(ClauseRole::ReturnClause))
		});
		if returns.is_some() != (output == OutputShape::Fields) {
			return Err(IrError::Invariant {
				what: "ir_lower::stage: a write holds a RETURN region exactly when it returns fields",
			});
		}
		let mut held = write.regions.iter().copied().filter(|region| Some(*region) != returns);
		let mut layers = Vec::new();
		if let ImageLayers::Stack(stack) = &stored.image_layers {
			for layer in stack {
				let columns: Vec<RegionId> = held.by_ref().take(layer.regions()).collect();
				layers.push(columns);
			}
		}
		if held.next().is_some()
			|| layers.iter().map(Vec::len).sum::<usize>()
				!= stored.image_layers.region_kinds().count()
		{
			return Err(IrError::Invariant {
				what: "ir_lower::stage: the field view regions of a write are not the layers it states",
			});
		}
		let mut cond = None;
		let mut data = None;
		let mut fields = None;
		let mut id_default = None;
		let mut indexes = None;
		let mut views = None;
		let mut repairs = None;
		let mut hosted: Vec<(WriteStageKind, Hosted)> = Vec::new();
		for (op, stage) in &ops {
			let regions =
				&ir.op(*op).ok_or(IrError::Stale("ir_lower::stage: a stage op is gone"))?.regions;
			let kind_of = |region: &RegionId| ir.region(*region).map(|held| held.kind);
			let held = Hosted {
				predicate: regions
					.iter()
					.copied()
					.find(|region| kind_of(region) == Some(RegionKind::PermissionFrame)),
				derivations: regions
					.iter()
					.copied()
					.filter(|region| {
						kind_of(region) == Some(RegionKind::FieldFrame(FieldRole::Computed))
					})
					.collect(),
			};
			if held != Hosted::default() && !hosted.iter().any(|(kind, _)| kind == stage) {
				hosted.push((*stage, held));
			}
			if *stage == WriteStageKind::ProcessTableFields && fields.is_none() {
				let counts = match stored.fields {
					Stored::Declared => stored.schema.region_counts(),
					Stored::Nothing | Stored::Dynamic | Stored::LateBound => {
						SchemaRegions::default()
					}
				};
				fields = Some(FieldRegions::of(ir, regions, counts)?);
			}
			if *stage == WriteStageKind::StoreIndexData && indexes.is_none() {
				let held = regions.iter().copied().skip(1).collect::<Vec<_>>();
				let stated = match stored.indexes {
					Stored::Declared => stored.index_set.region_count(),
					Stored::Nothing | Stored::Dynamic | Stored::LateBound => 0,
				};
				let columns = RegionKind::Clause(ClauseRole::IndexColumn);
				if held.len() != stated
					|| held.iter().any(|region| kind_of(region) != Some(columns))
				{
					return Err(IrError::Invariant {
						what: "ir_lower::stage: the index columns of a write are not the regions its indexes state",
					});
				}
				indexes = Some(held);
			}
			if matches!(stage, WriteStageKind::ProcessTableViews(_)) && views.is_none() {
				let held = regions.iter().copied().skip(1).collect::<Vec<_>>();
				let stated = match stored.views {
					Stored::Declared => stored.view_set.region_count(),
					Stored::Nothing | Stored::Dynamic | Stored::LateBound => 0,
				};
				let clauses = RegionKind::Clause(ClauseRole::ViewClause);
				if held.len() != stated
					|| held.iter().any(|region| kind_of(region) != Some(clauses))
				{
					return Err(IrError::Invariant {
						what: "ir_lower::stage: the view clauses of a write are not the regions its views state",
					});
				}
				views = Some(held);
			}
			if *stage == WriteStageKind::PurgeRecordData && repairs.is_none() {
				let held = regions.iter().copied().skip(1).collect::<Vec<_>>();
				let stated = match stored.referenced {
					Stored::Declared => stored.repair_set.region_count(),
					Stored::Nothing | Stored::Dynamic | Stored::LateBound => 0,
				};
				let clauses = RegionKind::Clause(ClauseRole::ReferenceRepair);
				if held.len() != stated
					|| held.iter().any(|region| kind_of(region) != Some(clauses))
				{
					return Err(IrError::Invariant {
						what: "ir_lower::stage: the repair clauses of a write are not the regions its references state",
					});
				}
				repairs = Some(held);
			}
			if *stage == WriteStageKind::GenerateRecordId && id_default.is_none() {
				id_default = regions.iter().copied().skip(1).find(|region| {
					kind_of(region) == Some(RegionKind::FieldFrame(FieldRole::Default))
				});
			}
			let clauses =
				regions.iter().copied().filter_map(|region| match ir.region(region)?.kind {
					RegionKind::Clause(role) => Some((region, role)),
					RegionKind::Request
					| RegionKind::Transaction(_)
					| RegionKind::Statement(_)
					| RegionKind::Block
					| RegionKind::ValueBlock
					| RegionKind::StatementIf
					| RegionKind::ValueIf
					| RegionKind::StatementFor
					| RegionKind::ValueFor
					| RegionKind::Loop
					| RegionKind::Subquery
					| RegionKind::FunctionBody
					| RegionKind::ClosureBody
					| RegionKind::PermissionFrame
					| RegionKind::FieldFrame(_)
					| RegionKind::EventFrame
					| RegionKind::SubscriberFrame
					| RegionKind::LazyOperand
					| RegionKind::ErrorScope(_)
					| RegionKind::ChooseArm
					| RegionKind::Pipeline
					| RegionKind::JoinLookupArm
					| RegionKind::WriteStage
					| RegionKind::CatalogUnit
					| RegionKind::Deadline
					| RegionKind::StoredClause(_) => None,
				});
			match stage {
				WriteStageKind::CheckWhereCondition => {
					for (region, role) in clauses {
						if role == ClauseRole::WriteCond && cond.replace(region).is_some() {
							return Err(IrError::Stale(
								"ir_lower::stage: a statement holds two conditions",
							));
						}
					}
				}
				WriteStageKind::ComputeInputData => {
					for (region, role) in clauses {
						if matches!(
							role,
							ClauseRole::SetValue
								| ClauseRole::Content | ClauseRole::DuplicateUpdate
						) && data.is_none()
						{
							data = Some(DataClause {
								region,
								role,
								kind: clause_kind(ir, region, role)?,
							});
						}
					}
				}
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
				| WriteStageKind::CheckSelectPermissions(_)
				| WriteStageKind::OutputWrite => {}
			}
		}
		Ok(Self {
			stages: ops.into_iter().map(|(_, stage)| stage).collect(),
			cond,
			data,
			output,
			returns,
			form,
			gates,
			hosted,
			derived,
			image,
			stored,
			fields: fields.unwrap_or_default(),
			id_default,
			indexes: indexes.unwrap_or_default(),
			views: views.unwrap_or_default(),
			repairs: repairs.unwrap_or_default(),
			layers,
			checked,
			nesting: statement_nesting(ir, write),
		})
	}
}

/// Where a statement stands below the statement the session runs, as the computation-depth count
/// of the events a write there fires is resumed from ([`StatementNesting::depth`]).
///
/// A statement the session runs stands nowhere. The count follows the regions that enclose the
/// statement: an arm of an `IF`, in a statement's place or a value's, is one arm; a block, in a
/// statement's place or a value's, or the body of a statement `FOR` nests what it holds; and a
/// statement in the position of any other expression (a value `FOR`, a subquery, a clause) is
/// held in an expression.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct StatementNesting {
	/// How many arms of an `IF` hold the statement.
	pub(crate) arms: u32,
	/// Whether a block or a loop body holds it.
	pub(crate) block: bool,
	/// Whether an expression holds it.
	pub(crate) expression: bool,
}

impl StatementNesting {
	/// Where a statement stands that `self` holds, standing at `inner` within it: the arms add up,
	/// and a block or an expression around either holds it.
	pub(crate) fn within(self, inner: StatementNesting) -> StatementNesting {
		StatementNesting {
			arms: self.arms + inner.arms,
			block: self.block || inner.block,
			expression: self.expression || inner.expression,
		}
	}

	/// The count: two for each arm, and one more where a block holds the statement, or an
	/// expression that no arm holds.
	pub(crate) fn depth(self) -> u32 {
		2 * self.arms + u32::from(self.block || (self.expression && self.arms == 0))
	}

	/// Where the regions from `region` outwards to the top of the container place what `region`
	/// holds.
	pub(crate) fn from_region(ir: &Ir, region: Option<RegionId>) -> StatementNesting {
		let mut nesting = StatementNesting::default();
		let mut region = region;
		while let Some(current) = region {
			match ir.region(current).map(|held| held.kind) {
				Some(RegionKind::StatementIf | RegionKind::ValueIf) => nesting.arms += 1,
				Some(RegionKind::Block | RegionKind::ValueBlock | RegionKind::StatementFor) => {
					nesting.block = true
				}
				Some(
					RegionKind::ValueFor
					| RegionKind::Subquery
					| RegionKind::Loop
					| RegionKind::Clause(_),
				) => nesting.expression = true,
				Some(
					RegionKind::Request
					| RegionKind::Transaction(_)
					| RegionKind::Statement(_)
					| RegionKind::FunctionBody
					| RegionKind::ClosureBody
					| RegionKind::PermissionFrame
					| RegionKind::FieldFrame(_)
					| RegionKind::EventFrame
					| RegionKind::SubscriberFrame
					| RegionKind::LazyOperand
					| RegionKind::ErrorScope(_)
					| RegionKind::ChooseArm
					| RegionKind::Pipeline
					| RegionKind::JoinLookupArm
					| RegionKind::WriteStage
					| RegionKind::CatalogUnit
					| RegionKind::Deadline
					| RegionKind::StoredClause(_),
				)
				| None => {}
			}
			region = ir
				.parent_op(current)
				.and_then(|owner| ir.op(owner))
				.and_then(|owner| ir.block(owner.parent))
				.map(|held| held.parent);
		}
		nesting
	}

	/// Where the regions around `region`, from the op that owns it outwards to the top of the
	/// container, place it: what a unit copied out of `region` stands within.
	pub(crate) fn around(ir: &Ir, region: RegionId) -> StatementNesting {
		let outer = ir
			.parent_op(region)
			.and_then(|owner| ir.op(owner))
			.and_then(|owner| ir.block(owner.parent))
			.map(|held| held.parent);
		Self::from_region(ir, outer)
	}
}

/// Where the write `write` stands in the container that holds it.
///
/// Only the container is read. A write that stands in a unit of its own (the region of a statement
/// in an expression, a body, an event's clause) sees the unit's top; where the unit stands is the
/// lowering context's ([`LowerCx::nesting`](super::units::LowerCx::nesting)).
fn statement_nesting(ir: &Ir, write: &OpData) -> StatementNesting {
	StatementNesting::from_region(ir, ir.block(write.parent).map(|held| held.parent))
}

/// What the data clause whose region is `region`, of role `role`, says: a `SET` and an `ON
/// DUPLICATE KEY UPDATE` hold assignments, an `UNSET` holds paths, and every other clause holds
/// the one op that says how it stores its value. An `INSERT`'s rows are no data clause.
fn clause_kind(ir: &Ir, region: RegionId, role: ClauseRole) -> Result<ClauseKind, IrError> {
	let mut assigns = false;
	let mut unsets = false;
	let mut content = None;
	for op in ops_in(ir, region) {
		match &ir.op(op).ok_or(IrError::Stale("ir_lower::stage: an op of a clause is gone"))?.kind {
			OpKind::Logical(LogOp::Assign(_)) | OpKind::Phys(PhysOp::Assign(_)) => assigns = true,
			OpKind::Logical(LogOp::Unset {
				..
			})
			| OpKind::Phys(PhysOp::Unset {
				..
			}) => unsets = true,
			OpKind::Logical(LogOp::Content {
				mode,
			})
			| OpKind::Phys(PhysOp::Content {
				mode,
			}) => {
				if content.replace(*mode).is_some() {
					return Err(IrError::Stale(
						"ir_lower::stage: a data clause holds two content ops",
					));
				}
			}
			OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_) => {}
		}
	}
	match (role, content) {
		(ClauseRole::SetValue | ClauseRole::DuplicateUpdate, None) if !unsets => {
			Ok(ClauseKind::Set)
		}
		(ClauseRole::Content, None) if !assigns && unsets => Ok(ClauseKind::Unset),
		// A clause whose value throws before the clause states what it writes raises when it is
		// evaluated, so how it would apply is never read.
		(ClauseRole::Content, None) if !assigns && raises_before_storing(ir, region) => {
			Ok(ClauseKind::Content(ContentMode::Content))
		}
		(ClauseRole::Content, Some(ContentMode::Single)) => Err(IrError::Invariant {
			what: "ir_lower::stage: an INSERT's rows are not a data clause",
		}),
		(ClauseRole::Content, Some(mode)) if !assigns && !unsets => Ok(ClauseKind::Content(mode)),
		_ => Err(IrError::Stale("ir_lower::stage: a data clause holds ops of another clause")),
	}
}

/// The stage sink of a write that names its one table: every stage emits its own instructions.
pub(super) struct Stages<'a, 'c> {
	/// The clauses the statement hosts on its stage ops.
	pub(super) run: &'a StageRun,
	/// The container the clauses are regions of.
	pub(super) ir: &'a Ir,
	/// The limits the host pipelines of the clauses are lowered under.
	pub(super) limits: LowerLimits,
	/// The function registry the host pipelines of the clauses resolve calls against where no
	/// context is installed.
	registry: &'a FunctionRegistry,
	/// The context the host pipelines of the clauses are lowered in: the function registry they
	/// resolve calls against, and the unit table the code they hold asks its units of. Without
	/// one, a host pipeline is lowered over `registry` and asks for no unit, and the field view
	/// of the written table is not lowered.
	pub(super) cx: Option<&'a mut LowerCx<'c>>,
	/// What the host pipelines of the stages emitted so far need of the program, which the program
	/// holds beside the needs of the body.
	pub(super) needs: Option<Needs>,
	/// The views the write maintains, lowered once for every stage that maintains them.
	views: Option<Arc<LoweredViews>>,
	/// The field view of the written table, lowered once for every stage that shows or reads an
	/// image through it.
	view: Option<(Option<u16>, bool)>,
	/// The guards on the stored fields of the written table, lowered once for every stage that
	/// reads the snapshot of a record with no pre-image.
	bare: Option<Option<u16>>,
}

impl<'a, 'c> Stages<'a, 'c> {
	pub(super) fn new(
		run: &'a StageRun,
		ir: &'a Ir,
		limits: LowerLimits,
		registry: &'a FunctionRegistry,
	) -> Self {
		Self {
			run,
			ir,
			limits,
			registry,
			cx: None,
			needs: None,
			views: None,
			view: None,
			bare: None,
		}
	}

	/// These stages with their host pipelines lowered in `cx`, whose limits they take.
	pub(super) fn in_context(mut self, cx: &'a mut LowerCx<'c>) -> Self {
		self.limits = cx.limits();
		self.cx = Some(cx);
		self
	}

	/// A host pipeline, lowered by `build` over a lowerer of `em`, that is beside the code in
	/// flight and holds none of its registers. What it needs of the program is kept in
	/// [`Self::needs`].
	fn host(
		&mut self,
		em: &mut Emitter,
		build: impl FnOnce(&mut Lowerer<'_>) -> Result<Pipeline, IrError>,
	) -> Result<u16, IrError> {
		let mut lowerer = match self.cx.as_deref_mut() {
			Some(cx) => Lowerer::in_context(em, self.ir, cx),
			None => Lowerer::new(em, self.ir, self.registry).with_limits(self.limits),
		};
		let aside = lowerer.em.set_aside();
		let beside = lowerer.beside();
		let built = build(&mut lowerer);
		lowerer.rejoin(beside);
		lowerer.em.restore(aside);
		let needs = lowerer.needs();
		let pipeline = built.and_then(|pipeline| lowerer.em.host_code(pipeline));
		self.needs = Some(match self.needs.take() {
			Some(held) => held.merged(&needs),
			None => needs,
		});
		pipeline
	}
}

impl Stages<'_, '_> {
	/// The field view of the written table as the instructions that show or read an image hold it:
	/// the host pipeline that applies it, and whether the caller's reads are permission-checked.
	///
	/// A plain view has no pipeline. A view that only cuts has none for a caller whose reads are
	/// not permission-checked, and neither has one whose layers the principal has none of. Any
	/// other view is the pipeline of the layers the write states
	/// ([`ImageLayers`]), lowered once, or, for a view with no layer form, the pipeline that has
	/// the host put an image through the table's field declarations. A view that cuts or derives
	/// and states neither is an invariant break: an image is never shown whole in its place.
	fn shown_view(&mut self, em: &mut Emitter) -> Result<(Option<u16>, bool), IrError> {
		if let Some(view) = self.view {
			return Ok(view);
		}
		let run = self.run;
		let view = match stated(run)? {
			ImageView::Plain => (None, run.checked),
			ImageView::Cut if !run.checked => (None, false),
			ImageView::Cut | ImageView::Derived => match &run.stored.image_layers {
				ImageLayers::Unstated => {
					return Err(IrError::Invariant {
						what: "ir_lower::stage: a field view that cuts or derives states no layers",
					});
				}
				ImageLayers::Stack(layers) if layers.is_empty() => (None, run.checked),
				ImageLayers::Stack(layers) => (Some(self.host_layers(em, layers)?), run.checked),
				ImageLayers::Enforced => (Some(self.host_enforced(em)?), run.checked),
			},
			ImageView::Dynamic | ImageView::LateBound => {
				return Err(IrError::Invariant {
					what: "ir_lower::stage: a view that is not stated passed the check for one",
				});
			}
		};
		self.view = Some(view);
		Ok(view)
	}

	/// The instruction that derives the pre-mutation snapshot of a record of the written table.
	///
	/// The snapshot is the post-image through the field view: a checked caller's is cut to the
	/// fields it may select, through the whole view where there is a pre-image and through the
	/// view's guards on stored fields alone ([`Self::bare_view`]) where there is none. For a plain
	/// view there is nothing to cut or derive, so neither pipeline exists.
	fn snapshot(&mut self, em: &mut Emitter) -> Result<DataInstr, IrError> {
		let (view, checked) = self.shown_view(em)?;
		let bare = if checked {
			self.bare_view(em)?
		} else {
			None
		};
		Ok(DataInstr::Snapshot {
			view,
			bare,
			checked,
		})
	}

	/// The instruction that derives the snapshot before the condition or the data clause reads it:
	/// [`Self::snapshot`], or, for an unchecked caller whose field view is one layer deriving the
	/// named components of the record id and nothing else, [`DataInstr::SnapshotKeyParts`] over the
	/// same view.
	fn deriving_snapshot(&mut self, em: &mut Emitter) -> Result<DataInstr, IrError> {
		let snapshot = self.snapshot(em)?;
		let DataInstr::Snapshot {
			view: Some(view),
			checked: false,
			..
		} = snapshot
		else {
			return Ok(snapshot);
		};
		let ImageLayers::Stack(layers) = &self.run.stored.image_layers else {
			return Ok(snapshot);
		};
		Ok(match key_parts(self.ir, layers, &self.run.layers) {
			Some(parts) => DataInstr::SnapshotKeyParts {
				parts,
				view: Some(view),
			},
			None => snapshot,
		})
	}

	/// The host pipeline that cuts from an image of a record with no pre-image each stored field a
	/// checked caller may not select, deriving nothing: the columns of the layers the write states
	/// ([`ImageLayers`]) that guard a field the layer's input does not declare `COMPUTED`, or, for
	/// a view with no layer form, the table's field permissions. `None` where the view is plain or
	/// guards no stored field.
	fn bare_view(&mut self, em: &mut Emitter) -> Result<Option<u16>, IrError> {
		if let Some(bare) = self.bare {
			return Ok(bare);
		}
		let run = self.run;
		let bare = match stated(run)? {
			ImageView::Plain => None,
			ImageView::Cut | ImageView::Derived => match &run.stored.image_layers {
				ImageLayers::Unstated => {
					return Err(IrError::Invariant {
						what: "ir_lower::stage: a field view that cuts or derives states no layers",
					});
				}
				ImageLayers::Stack(layers) => self.host_stored_guards(em, layers)?,
				ImageLayers::Enforced => Some(self.host_enforced_as(em, Enforce::Guards)?),
			},
			ImageView::Dynamic | ImageView::LateBound => {
				return Err(IrError::Invariant {
					what: "ir_lower::stage: a view that is not stated passed the check for one",
				});
			}
		};
		self.bare = Some(bare);
		Ok(bare)
	}

	/// The host pipeline that applies the layers `layers` of the field view over the image the
	/// host hands it, lowered from the regions the write hosts for them. What it needs of the
	/// program is kept in [`Self::needs`].
	fn host_layers(
		&mut self,
		em: &mut Emitter,
		layers: &[SubscriberLayer],
	) -> Result<u16, IrError> {
		let built = self.host_layer_stack(em, layers, |em, cx, ir, held| {
			lower_layers(em, cx, ir, held).map(Some)
		})?;
		built.ok_or(IrError::Invariant {
			what: "ir_lower::stage: the layers of the field view lowered to no pipeline",
		})
	}

	/// The host pipeline that applies the columns of the layers `layers` of the field view that
	/// guard a stored field over the image the host hands it, or `None` where no column does.
	fn host_stored_guards(
		&mut self,
		em: &mut Emitter,
		layers: &[SubscriberLayer],
	) -> Result<Option<u16>, IrError> {
		self.host_layer_stack(em, layers, lower_stored_guards)
	}

	/// The host pipeline `lower` lowers from the layers `layers` of the field view and the regions
	/// the write hosts for them, beside the code in flight. What it needs of the program is kept
	/// in [`Self::needs`].
	fn host_layer_stack(
		&mut self,
		em: &mut Emitter,
		layers: &[SubscriberLayer],
		lower: impl FnOnce(
			&mut Emitter,
			&mut LowerCx<'_>,
			&Ir,
			&[(&SubscriberLayer, &[RegionId])],
		) -> Result<Option<(Pipeline, Needs)>, IrError>,
	) -> Result<Option<u16>, IrError> {
		let run = self.run;
		let ir = self.ir;
		let held: Vec<(&SubscriberLayer, &[RegionId])> =
			layers.iter().zip(&run.layers).map(|(layer, regions)| (layer, &regions[..])).collect();
		if held.len() != layers.len() {
			return Err(IrError::Invariant {
				what: "ir_lower::stage: a layer of the field view holds no regions",
			});
		}
		let aside = em.set_aside();
		let built = match self.cx.as_deref_mut() {
			Some(cx) => lower(em, cx, ir, &held),
			None => Err(invariant("a field view lowered with no lowering context")),
		};
		em.restore(aside);
		let Some((pipeline, needs)) = built? else {
			return Ok(None);
		};
		self.needs = Some(match self.needs.take() {
			Some(held) => held.merged(&needs),
			None => needs,
		});
		em.host_code(pipeline).map(Some)
	}

	/// The host pipeline that has the host put an image through the field declarations of the
	/// written table: a view with no layer form. It holds no code but the emission of the image.
	fn host_enforced(&mut self, em: &mut Emitter) -> Result<u16, IrError> {
		self.host_enforced_as(em, Enforce::Image)
	}

	/// The host pipeline that has the host put an image through the field declarations of the
	/// written table as `enforce` names them: a view with no layer form.
	fn host_enforced_as(
		&mut self,
		em: &mut Emitter,
		enforce: fn(TableName) -> Enforce,
	) -> Result<u16, IrError> {
		let table = self.run.stored.table.clone().ok_or(IrError::Invariant {
			what: "ir_lower::stage: a field view with no layer form names no table",
		})?;
		let aside = em.set_aside();
		em.emit(Instr::Emit {
			src: ROW,
		});
		em.emit(Instr::EndRow);
		let built = em.finish_pipeline(Source::Once);
		em.restore(aside);
		let mut pipeline = built?;
		pipeline.enforce = Some(enforce(table));
		em.host_code(pipeline)
	}
}

impl StageSink for Stages<'_, '_> {
	fn stages(
		&mut self,
		em: &mut Emitter,
		stages: &[WriteStageKind],
		skip: Label,
	) -> Result<(), IrError> {
		for stage in stages {
			emit_stage(em, self, *stage, skip)?;
		}
		Ok(())
	}
}

/// A storage instruction, as a write instruction.
fn storage(instr: StorageInstr) -> Instr {
	Instr::Write(WriteInstr::Storage(instr))
}

/// A data instruction, as a write instruction.
fn data(instr: DataInstr) -> Instr {
	Instr::Write(WriteInstr::Data(instr))
}

/// A schema instruction, as a write instruction.
fn schema(instr: SchemaInstr) -> Instr {
	Instr::Write(WriteInstr::Schema(instr))
}

/// Answers `Ok` when the table stores none of the kind `kind` states, so the stage that maintains
/// it has no clause to lower, and otherwise why the stage cannot lower: it holds clauses, which
/// the arm of a stage that has them lowers and never asks here ([`IrError::Invariant`] naming
/// `what`), the write names no one table (`NotYet`), or the definitions read predate a catalog
/// change ([`IrError::LateBind`]).
fn no_clauses(kind: Stored, what: &'static str) -> Result<(), IrError> {
	match kind {
		Stored::Nothing => Ok(()),
		Stored::Declared => Err(IrError::Invariant {
			what,
		}),
		Stored::Dynamic => {
			Err(not_yet("ir_lower::stage: a stage of a write that names no one table"))
		}
		Stored::LateBound => Err(IrError::LateBind {
			what: "ir_lower::stage: the definitions of a table read before a catalog change",
		}),
	}
}

/// The schema plan of a written table that declares no field: its name, and whether it is
/// `SCHEMAFULL`.
fn schema_plan(run: &StageRun) -> Result<Arc<SchemaPlan>, IrError> {
	let table = run.stored.table.clone().ok_or(IrError::Invariant {
		what: "ir_lower::stage: a stated table fact with no table",
	})?;
	Ok(Arc::new(SchemaPlan {
		table,
		schemafull: run.stored.schemafull,
		id_field: None,
		fields: Vec::new(),
	}))
}

/// The schema plan of a written table that declares fields: its name, whether it is `SCHEMAFULL`,
/// the position of its `id` field and every field it declares, in the order the catalog holds
/// them.
fn declared_plan(run: &StageRun) -> Result<Arc<SchemaPlan>, IrError> {
	let table = run.stored.table.clone().ok_or(IrError::Invariant {
		what: "ir_lower::stage: a stated table fact with no table",
	})?;
	let declared = &run.stored.schema;
	if declared.fields.is_empty() {
		return Err(IrError::Invariant {
			what: "ir_lower::stage: a table that declares fields states none",
		});
	}
	Ok(Arc::new(SchemaPlan {
		table,
		schemafull: run.stored.schemafull,
		id_field: declared.id_field.map(usize::from),
		fields: declared
			.fields
			.iter()
			.map(|name| FieldPlan {
				name: name.as_str().to_owned(),
			})
			.collect(),
	}))
}

/// Jumps to `target` when `fact` holds of the record, or when it does not hold.
fn jump_if(em: &mut Emitter, fact: StorageFact, when: bool, target: Label) {
	em.emit(storage(StorageInstr::JumpIf {
		fact,
		when,
		target,
	}));
}

/// Whether the run's write discards a record of a `DROP` table: it stores through
/// `StoreRecordData`, which writes nothing for one. Every other write changes storage whatever the
/// table's `DROP` clause says, as a delete's purge removes the stored record, so a structure kept
/// per stored record follows the record there.
fn discards_dropped_records(run: &StageRun) -> bool {
	run.stages.contains(&WriteStageKind::StoreRecordData)
}

/// A permission-gate instruction, as a write instruction.
fn gate(instr: GateInstr) -> Instr {
	Instr::Write(WriteInstr::Permissions(instr))
}

/// What a gate stage does with a clause the write states.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Verdict {
	/// Every record is admitted.
	Admit,
	/// Every record is refused to a caller whose actions are permission-checked.
	Refuse,
	/// The clause is a predicate, which the gate stage hosts. `derives` is whether it may read a
	/// `COMPUTED` field of the table.
	Predicate {
		derives: bool,
	},
}

/// What `clause` says of a gate stage, or the refusal for a clause the write does not state.
fn verdict(clause: GateClause) -> Result<Verdict, IrError> {
	match clause {
		GateClause::Full => Ok(Verdict::Admit),
		GateClause::None | GateClause::Absent => Ok(Verdict::Refuse),
		GateClause::Predicate {
			derives,
		} => Ok(Verdict::Predicate {
			derives,
		}),
		GateClause::Dynamic => {
			Err(not_yet("ir_lower::stage: a permission gate of a write that names no one table"))
		}
		GateClause::LateBound => Err(IrError::LateBind {
			what: "ir_lower::stage: a permission clause read before a catalog change",
		}),
		GateClause::Unlowerable => Err(IrError::Invariant {
			what: "ir_lower::stage: a permission clause with no logical form",
		}),
	}
}

/// Skips every record the caller's `action`s are permission-checked for. A record with no id is a
/// temporary one that no gate judges.
fn refuse(em: &mut Emitter, action: Action, skip: Label) -> Result<(), IrError> {
	let passed = em.label()?;
	em.emit(gate(GateInstr::JumpIfNoId {
		target: passed,
	}));
	em.emit(gate(GateInstr::JumpIfUnchecked {
		action,
		target: passed,
	}));
	em.emit(Instr::Jump {
		target: skip,
	});
	em.bind(passed)
}

/// What a gate stage judges: the clause the write states for `stage`, over a copy of `image`, for
/// a caller whose `action`s are permission-checked, in the gate frame of `guard`.
#[derive(Clone, Copy)]
struct Judgement<'a> {
	stage: WriteStageKind,
	clause: GateClause,
	/// The computed fields a predicate of the clause derives onto the copy first.
	derives: &'a [DerivedField],
	guard: GuardKind,
	image: RecordImage,
	action: Action,
}

impl<'a> Judgement<'a> {
	/// A write's clause, judged against the record the stages built.
	fn write(stage: WriteStageKind, clause: GateClause, derives: &'a [DerivedField]) -> Self {
		Self {
			stage,
			clause,
			derives,
			guard: GuardKind::Write,
			image: RecordImage::Current,
			action: Action::Edit,
		}
	}

	/// The select clause, judged against `image` as it stands.
	fn select(
		stage: WriteStageKind,
		clause: GateClause,
		derives: &'a [DerivedField],
		image: RecordImage,
	) -> Self {
		Self {
			stage,
			clause,
			derives,
			guard: GuardKind::Read,
			image,
			action: Action::View,
		}
	}
}

/// A gate over what `judgement` names: a `FULL` clause admits, a refusing one skips the record,
/// and a predicate is judged and skips the record when it is falsy.
///
/// A record with no id is a temporary one that no gate judges. A predicate that may read a
/// `COMPUTED` field is judged by a second host pipeline that derives each field onto the copy, in
/// the order the write states them, before the verdict, whatever the image carries under their
/// names: a value the statement assigned, or one derived for the caller. The image itself is
/// stored or shown as it stands, so a field is never derived on it. A field under an `AUTH LIMIT`
/// whose body is lowered for a principal answers `NotYet`.
fn judge(
	em: &mut Emitter,
	cx: &mut Stages<'_, '_>,
	judgement: Judgement<'_>,
	skip: Label,
) -> Result<(), IrError> {
	let Judgement {
		stage,
		clause,
		derives,
		guard,
		image,
		action,
	} = judgement;
	let run = cx.run;
	match verdict(clause)? {
		Verdict::Admit => Ok(()),
		Verdict::Refuse => refuse(em, action, skip),
		Verdict::Predicate {
			derives: states,
		} => {
			if states == derives.is_empty() {
				return Err(IrError::Invariant {
					what: "ir_lower::stage: a permission clause states a derivation its fields contradict",
				});
			}
			let hosted = run
				.hosted
				.iter()
				.find_map(|(held, hosted)| (*held == stage).then_some(hosted))
				.ok_or(IrError::Invariant {
					what: "ir_lower::stage: a permission predicate its gate stage does not host",
				})?;
			let predicate = hosted.predicate.ok_or(IrError::Invariant {
				what: "ir_lower::stage: a permission predicate its gate stage does not host",
			})?;
			let fields = derivations(derives, &hosted.derivations)?;
			let plain = cx.host(em, |lowerer| gate_pipeline(lowerer, predicate, guard, &[]))?;
			let derived = match fields.is_empty() {
				true => None,
				false => {
					Some(cx.host(em, |lowerer| gate_pipeline(lowerer, predicate, guard, &fields))?)
				}
			};
			let passed = em.label()?;
			em.emit(gate(GateInstr::JumpIfNoId {
				target: passed,
			}));
			em.emit(gate(GateInstr::JumpIfUnchecked {
				action,
				target: passed,
			}));
			em.emit(gate(GateInstr::Judge {
				image,
				plain,
				derived,
				skip,
			}));
			if stage == WriteStageKind::CheckSelectPermissions(RecordImage::Current)
				&& returns_reads_before(cx)
			{
				jump_if(em, StorageFact::IterationInitial, true, passed);
				em.emit(gate(GateInstr::Judge {
					image: RecordImage::Initial,
					plain,
					derived,
					skip,
				}));
			}
			em.bind(passed)
		}
	}
}

/// Whether the statement's `RETURN` clause reads the pre-mutation image (`$before`).
///
/// When it does, `CheckSelectPermissions(RecordImage::Current)` must also judge an existing
/// record's pre-mutation image against the table's `SELECT` predicate so a mutation that moves a
/// record into the caller's `SELECT` scope cannot disclose its prior state through `RETURN BEFORE`,
/// `RETURN DIFF`, or a `RETURN` projection referencing `$before`.
fn returns_reads_before(cx: &Stages<'_, '_>) -> bool {
	match cx.run.output {
		OutputShape::Before | OutputShape::Diff => true,
		OutputShape::Fields => cx
			.run
			.returns
			.and_then(|region| cx.ir.region(region))
			.and_then(|r| r.blocks.first())
			.and_then(|&b| cx.ir.block(b))
			.and_then(|b| b.args.get(1))
			.is_some_and(|&before| !cx.ir.uses(before).is_empty()),
		OutputShape::None | OutputShape::Null | OutputShape::Default | OutputShape::After => false,
	}
}

/// The fields `fields` pair with the regions `regions` that derive them, in order.
///
/// A stage hosts one region per field its write states, so a count that differs is a broken
/// invariant, never a field derived from the wrong body or left out.
fn derivations<'f>(
	fields: &'f [DerivedField],
	regions: &[RegionId],
) -> Result<Vec<(&'f DerivedField, RegionId)>, IrError> {
	if fields.len() != regions.len() {
		return Err(IrError::Invariant {
			what: "ir_lower::stage: a stage hosts a region for each computed field it derives",
		});
	}
	Ok(fields.iter().zip(regions.iter().copied()).collect())
}

/// The refusal of a target whose own permission for the statement is `NONE`, ahead of any record
/// being read: such a target yields no record to write, so the statement answers no rows and no
/// refusal. A table the catalog does not define, a clause that is `FULL` or a predicate, and the
/// forms whose subjects are not read from a target take none.
fn target_refusal(em: &mut Emitter, run: &StageRun, skip: Label) -> Result<(), IrError> {
	let clause = match run.form {
		WriteForm::Create => run.gates.create,
		WriteForm::Update | WriteForm::Upsert => run.gates.update,
		WriteForm::Delete => run.gates.delete,
		WriteForm::Insert | WriteForm::Relate => return Ok(()),
	};
	if clause == GateClause::Absent || verdict(clause)? != Verdict::Refuse {
		return Ok(());
	}
	let judged = em.label()?;
	em.emit(gate(GateInstr::JumpIfUnchecked {
		action: Action::Edit,
		target: judged,
	}));
	em.emit(Instr::Jump {
		target: skip,
	});
	em.bind(judged)
}

/// Emits `stage` into `em`, jumping to `skip` where it abandons the record.
///
/// Every stage has an arm of its own. A stage that needs a clause the lowering has no form for
/// answers [`IrError::NotYet`] naming it.
pub(super) fn emit_stage(
	em: &mut Emitter,
	cx: &mut Stages<'_, '_>,
	stage: WriteStageKind,
	skip: Label,
) -> Result<(), IrError> {
	let run = cx.run;
	match stage {
		// A record the statement named that storage does not hold is skipped, not failed.
		WriteStageKind::CheckRecordExists => jump_if(em, StorageFact::Absent, true, skip),
		WriteStageKind::CheckTableNotView => {
			em.emit(storage(StorageInstr::RefuseView));
		}
		WriteStageKind::CheckTableNotLightweight(reason) => {
			em.emit(storage(StorageInstr::RefuseLightweight {
				reason,
			}));
		}
		WriteStageKind::CheckTableType(subject) => {
			em.emit(storage(StorageInstr::RefuseTableType {
				subject,
			}));
		}
		WriteStageKind::ClearRecordData => {
			em.emit(storage(StorageInstr::Clear));
		}
		// Nothing is written for a record no mutation phase changed, a record of a `DROP` table,
		// or an edge of a lightweight relation, whose whole state is its adjacency keys. A record
		// written for the first time in its iteration is written create-only, so a caller holding
		// `create` alone cannot overwrite one it may not update.
		WriteStageKind::StoreRecordData => {
			// Every gate that skips a record before it writes is behind it.
			em.emit(Instr::Write(WriteInstr::ReachedStore));
			let unwritten = em.label()?;
			for fact in
				[StorageFact::Unmodified, StorageFact::TableDropped, StorageFact::TableLightweight]
			{
				jump_if(em, fact, true, unwritten);
			}
			let overwrite = em.label()?;
			jump_if(em, StorageFact::IterationInitial, false, overwrite);
			em.emit(storage(StorageInstr::Store {
				create: true,
				cache: keeps_created(run),
			}));
			em.emit(Instr::Jump {
				target: unwritten,
			});
			em.bind(overwrite)?;
			em.emit(storage(StorageInstr::Store {
				create: false,
				cache: false,
			}));
			em.bind(unwritten)?;
		}
		// An edge the statement relates writes its adjacency keys. An enforced relation admits it
		// only between endpoints that exist, and no edge may hang off a lightweight relation's
		// edge. A lightweight relation writes its two pointer keys alone; a stored edge of the
		// original layout first loses the pointer keys that layout wrote.
		WriteStageKind::StoreEdgesData => {
			let end = em.label()?;
			jump_if(em, StorageFact::TableDropped, true, end);
			jump_if(em, StorageFact::Related, false, end);
			let unenforced = em.label()?;
			jump_if(em, StorageFact::EnforcedRelation, false, unenforced);
			em.emit(storage(StorageInstr::RequireEndpoints));
			em.bind(unenforced)?;
			em.emit(storage(StorageInstr::RefuseLightweightEndpoints));
			let lightweight = em.label()?;
			jump_if(em, StorageFact::TableLightweight, true, lightweight);
			let current = em.label()?;
			jump_if(em, StorageFact::LegacyLayout, false, current);
			em.emit(storage(StorageInstr::DeleteLegacyPointers));
			em.bind(current)?;
			em.emit(storage(StorageInstr::WriteEdgeKeys {
				lightweight: false,
			}));
			let written = em.label()?;
			em.emit(Instr::Jump {
				target: written,
			});
			em.bind(lightweight)?;
			em.emit(storage(StorageInstr::WriteEdgeKeys {
				lightweight: true,
			}));
			em.bind(written)?;
			em.emit(storage(StorageInstr::CacheEdge));
			em.emit(storage(StorageInstr::PinEndpoints));
			em.bind(end)?;
		}
		// A stored edge of the current layout has its pointer values restamped with the table's
		// inline payload on every write but its relate, and may not move its endpoints doing so.
		WriteStageKind::StoreInlineAdjacencyData => {
			let end = em.label()?;
			for (fact, when) in [
				(StorageFact::Related, true),
				(StorageFact::CurrentLayout, false),
				(StorageFact::TableDropped, true),
				(StorageFact::TableLightweight, true),
				(StorageFact::NoInlineFields, true),
			] {
				jump_if(em, fact, when, end);
			}
			em.emit(storage(StorageInstr::RequireUnmovedEndpoints));
			em.emit(storage(StorageInstr::RewriteInlinePointers));
			em.bind(end)?;
		}
		// The record's doc-id released once every index has let go of it: marked for the build's
		// replay when an in-flight build took the record's delete, and otherwise dropped from the
		// table's shared doc-id space, unless nothing on the table consumes that space, as the
		// transaction's settled read of the table's graph marker confirms. The purge removes the
		// stored record of a `DROP` table as it does any other, so its doc-id goes with it.
		WriteStageKind::ReclaimDocId => {
			let release = em.label()?;
			jump_if(em, StorageFact::DocIdDeferred, false, release);
			em.emit(storage(StorageInstr::MarkDocIdPending));
			let done = em.label()?;
			em.emit(Instr::Jump {
				target: done,
			});
			em.bind(release)?;
			let consumed = em.label()?;
			jump_if(em, StorageFact::DocIdConsumer, true, consumed);
			jump_if(em, StorageFact::GraphDocIdsSettled, false, done);
			em.bind(consumed)?;
			em.emit(storage(StorageInstr::ReleaseDocId));
			em.bind(done)?;
		}
		// The record's change buffered for the change feed and the live-query capture, and the
		// table's live subscriptions notified of it.
		WriteStageKind::ProcessChangefeeds => {
			em.emit(Instr::Write(WriteInstr::Observe(ObserveInstr::RecordChange)));
		}
		WriteStageKind::ProcessTableLives(action) => {
			em.emit(Instr::Write(WriteInstr::Observe(ObserveInstr::Lives(action))));
		}
		// A statement with no condition matches every record it reads; one with a condition
		// judges the snapshot of the record against it, and skips the record it rejects.
		WriteStageKind::CheckWhereCondition => match run.cond {
			None => {
				em.emit(data(DataInstr::Matched));
			}
			Some(region) => {
				let snapshot = cx.deriving_snapshot(em)?;
				let pipeline = cx.host(em, |lowerer| condition_pipeline(lowerer, region))?;
				em.emit(data(snapshot));
				em.emit(data(DataInstr::Where {
					pipeline,
					skip,
				}));
			}
		},
		// A record whose data clause was evaluated already, by an earlier stage or order, is not
		// evaluated again. An `UNSET` evaluates nothing, and a statement with no clause no stage.
		WriteStageKind::ComputeInputData => {
			if let Some(clause) = run.data {
				let snapshot = cx.deriving_snapshot(em)?;
				let pipeline = match clause.kind {
					ClauseKind::Unset => None,
					ClauseKind::Set => {
						Some(cx.host(em, |lowerer| data_pipeline(lowerer, clause.region, true))?)
					}
					ClauseKind::Content(_) => {
						Some(cx.host(em, |lowerer| data_pipeline(lowerer, clause.region, false))?)
					}
				};
				let evaluated = em.label()?;
				em.emit(data(DataInstr::JumpIfInput {
					target: evaluated,
				}));
				em.emit(data(snapshot));
				em.emit(data(DataInstr::ComputeInput {
					pipeline,
					binds_input: clause.role == ClauseRole::DuplicateUpdate,
				}));
				em.bind(evaluated)?;
			}
		}
		// A `PATCH` is judged against the snapshot first, so that a pointer that reads a field the
		// caller may not select is refused; any other clause applies as evaluated.
		WriteStageKind::ProcessRecordData => {
			let patch = run
				.data
				.is_some_and(|clause| clause.kind == ClauseKind::Content(ContentMode::Patch));
			if patch {
				let DataInstr::Snapshot {
					view,
					bare,
					checked,
				} = cx.snapshot(em)?
				else {
					return Err(IrError::Invariant {
						what: "ir_lower::stage: the snapshot of a PATCH is no snapshot",
					});
				};
				em.emit(data(DataInstr::ApplyPatch {
					view,
					bare,
					checked,
				}));
			} else {
				em.emit(data(DataInstr::ApplyInput));
			}
		}
		WriteStageKind::CheckDataFields => {
			em.emit(data(DataInstr::CheckDataFields));
		}
		WriteStageKind::ProcessMergeData => {
			em.emit(data(DataInstr::MergeSupplied));
		}
		// The table whose `id` names the components of its record ids judges each record against
		// its key.
		WriteStageKind::DefaultRecordData => {
			stated(run)?;
			em.emit(data(DataInstr::StampRecordFields));
			if run.image.keyed {
				em.emit(data(DataInstr::SettleKeyFields));
			}
		}
		// A `DEFAULT` of the `id` field is the field's own code, run in its privileged frame: a
		// host pipeline over the record the `DEFAULT` reads, which the stage hosts as a region.
		WriteStageKind::GenerateRecordId => {
			stated(run)?;
			let default = match (run.image.id_default, run.id_default) {
				(false, None) => None,
				(true, Some(region)) => {
					let clause = FieldClause {
						caller_bound: run.stored.schema.id_default_bound,
					};
					Some(cx.host(em, |lowerer| field_clause_pipeline(lowerer, region, clause))?)
				}
				(true, None) | (false, Some(_)) => {
					return Err(IrError::Invariant {
						what: "ir_lower::stage: the id DEFAULT its image states and the region its stage hosts disagree",
					});
				}
			};
			em.emit(data(DataInstr::GenerateId {
				default,
			}));
		}
		// Tenant confinement runs first, so a caller outside its tenant is refused whatever the
		// target's own permission says.
		WriteStageKind::CheckRecordUserAccess => {
			em.emit(gate(GateInstr::Tenant));
			target_refusal(em, run, skip)?;
		}
		// Only a `NONE` create permission refuses ahead of the data clause; a predicate is
		// judged after it, against the record it built.
		WriteStageKind::CheckPermissionsQuickCreate => {
			if verdict(run.gates.create)? == Verdict::Refuse {
				refuse(em, Action::Edit, skip)?;
			}
		}
		WriteStageKind::CheckCreatePermissions => {
			let derives = &run.derived.create;
			judge(em, cx, Judgement::write(stage, run.gates.create, derives), skip)?;
		}
		WriteStageKind::CheckUpdatePermissions => {
			let derives = &run.derived.update;
			judge(em, cx, Judgement::write(stage, run.gates.update, derives), skip)?;
		}
		WriteStageKind::CheckDeletePermissions => {
			let derives = &run.derived.delete;
			judge(em, cx, Judgement::write(stage, run.gates.delete, derives), skip)?;
		}
		// `FULL` and `NONE` were settled by the check before the write, so only a predicate is
		// judged again, against the record as the update left it.
		WriteStageKind::RecheckUpdatePermissions => {
			if matches!(verdict(run.gates.update)?, Verdict::Predicate { .. }) {
				let derives = &run.derived.update;
				judge(em, cx, Judgement::write(stage, run.gates.update, derives), skip)?;
			}
		}
		WriteStageKind::CheckSelectPermissions(image) => {
			let derives = &run.derived.select;
			judge(em, cx, Judgement::select(stage, run.gates.select, derives, image), skip)?;
		}
		// A table with no field declaration applies no field: the schema is opened and closed
		// around nothing, which still refuses a field an undeclared `SCHEMAFULL` table lacks. A
		// table that declares fields applies each one's clauses in the order its schema states.
		WriteStageKind::ProcessTableFields => match run.stored.fields {
			Stored::Declared => process_fields(em, cx)?,
			Stored::Nothing | Stored::Dynamic | Stored::LateBound => {
				no_clauses(run.stored.fields, "the field declarations of the written table")?;
				let table = schema_plan(run)?;
				let end = em.label()?;
				em.emit(schema(SchemaInstr::Begin {
					table,
					end,
				}));
				em.emit(schema(SchemaInstr::Finish {
					computed: Arc::from(Vec::new()),
				}));
				em.bind(end)?;
			}
		},
		WriteStageKind::CleanupTableFields => {
			let table = match run.stored.fields {
				Stored::Declared => declared_plan(run)?,
				Stored::Nothing | Stored::Dynamic | Stored::LateBound => {
					no_clauses(run.stored.fields, "the field declarations of the written table")?;
					schema_plan(run)?
				}
			};
			em.emit(schema(SchemaInstr::Cleanup {
				table,
			}));
		}
		// Every `COMPUTED` field of the table is derived onto both images of a record that has an
		// observer, in the table's derivation order; a table with none derives nothing.
		WriteStageKind::MaterialiseObservedFields => match run.stored.fields {
			Stored::Declared => observed(em, cx, stage)?,
			Stored::Nothing | Stored::Dynamic | Stored::LateBound => {
				no_clauses(run.stored.fields, "the computed fields of the written table")?;
			}
		},
		// The reference keys the table's `REFERENCE` fields declare are kept in step with the
		// record storage holds: synced on a write and released on a delete. A write that discards
		// a record of a `DROP` table leaves the keys as storage leaves the record.
		WriteStageKind::ProcessTableReferences | WriteStageKind::CleanupTableReferences => {
			match run.stored.references {
				Stored::Declared => {
					if run.stored.reference_fields.is_empty() {
						return Err(IrError::Invariant {
							what: "ir_lower::stage: a table that declares references states no field",
						});
					}
					let release = stage == WriteStageKind::CleanupTableReferences;
					let references = |instr| storage(StorageInstr::References(instr));
					let sync = storage(StorageInstr::SyncReferences {
						fields: run.stored.reference_fields.iter().cloned().collect(),
						release,
					});
					let materialize = materialize_steps(cx, run)?;
					if discards_dropped_records(run) {
						let synced = em.label()?;
						jump_if(em, StorageFact::TableDropped, true, synced);
						em.emit(sync);
						if let Some(steps) = materialize {
							em.emit(references(ReferenceInstr::Materialize {
								steps,
								release,
							}));
						}
						em.bind(synced)?;
					} else {
						em.emit(sync);
						if let Some(steps) = materialize {
							em.emit(references(ReferenceInstr::Materialize {
								steps,
								release,
							}));
						}
					}
				}
				Stored::Nothing | Stored::Dynamic | Stored::LateBound => {
					no_clauses(run.stored.references, "the reference fields of the written table")?;
				}
			}
		}
		// Every index of the table is kept in line with the record's images. A table with none
		// keeps no entries for a statement that cannot define one, and for any other the stage
		// still checks, against the indexes the table holds when the record is written, that the
		// table has not gained one. A table with some maintains each from the values its columns
		// answer for the two images.
		WriteStageKind::StoreIndexData if run.stored.indexes == Stored::Declared => {
			store_indexes(em, cx)?;
		}
		WriteStageKind::StoreIndexData => {
			no_clauses(run.stored.indexes, "the indexes of the written table")?;
			if !run.stored.catalog_fixed {
				let end = em.label()?;
				for fact in index_skips(run) {
					jump_if(em, *fact, true, end);
				}
				em.emit(storage(StorageInstr::CheckIndexes {
					lowered: Arc::new(LoweredIndexes::new(Arc::from(Vec::new()), Vec::new())),
				}));
				em.bind(end)?;
			}
		}
		// A deleted record's storage is taken back out, and the references to it repaired.
		WriteStageKind::PurgeRecordData => purge_stage(em, cx)?,
		// The views the table feeds are maintained for the record; a table that feeds none holds
		// the stage's instruction with no view to maintain.
		WriteStageKind::ProcessTableViews(action) => views_stage(em, cx, action)?,
		// The events the table holds are one unit, which runs each event's clauses for the record.
		// A table with none runs none for a statement that cannot define one.
		WriteStageKind::ProcessTableEvents(action) => events_stage(em, cx, action)?,
		// Nothing is shown or written under `OPTION IMPORT`. Any other shape shows the record's
		// images through the written table's field view. An unspecified `RETURN` is the
		// post-image on the orders that leave one, and nothing on the others.
		WriteStageKind::OutputWrite => {
			let show = |image, (view, checked)| {
				Instr::Write(WriteInstr::ShowImage {
					image,
					view,
					checked,
				})
			};
			let set = |output| {
				Instr::Write(WriteInstr::SetOutput {
					output,
				})
			};
			let imported = em.label()?;
			let shown = match run.output {
				OutputShape::None | OutputShape::Null => None,
				OutputShape::Default
				| OutputShape::Diff
				| OutputShape::After
				| OutputShape::Before
				| OutputShape::Fields => Some(cx.shown_view(em)?),
			};
			// The projection compiles before any instruction is emitted, so a refused `RETURN`
			// leaves nothing half-emitted.
			let projection = match run.output {
				OutputShape::Fields => return_pipeline(em, cx)?,
				OutputShape::None
				| OutputShape::Null
				| OutputShape::Default
				| OutputShape::Diff
				| OutputShape::After
				| OutputShape::Before => None,
			};
			em.emit(Instr::Write(WriteInstr::JumpIfImport {
				target: imported,
			}));
			em.emit(Instr::Write(WriteInstr::ClearCurrentReduced));
			match (run.output, shown) {
				(OutputShape::None, _) => {
					em.emit(set(WriteOutput::None));
				}
				(OutputShape::Null, _) => {
					em.emit(set(WriteOutput::Null));
				}
				(OutputShape::After, Some(view)) => {
					em.emit(show(RecordImage::Current, view));
					em.emit(set(WriteOutput::Shown(RecordImage::Current)));
				}
				(OutputShape::Before, Some(view)) => {
					em.emit(show(RecordImage::Initial, view));
					em.emit(set(WriteOutput::Shown(RecordImage::Initial)));
				}
				(OutputShape::Diff, Some(view)) => {
					em.emit(show(RecordImage::Initial, view));
					em.emit(show(RecordImage::Current, view));
					em.emit(set(WriteOutput::Diff));
				}
				// Both images are shown, because the projection reads them as `$after` and
				// `$before`. A `RETURN *` alone is the post-image itself.
				(OutputShape::Fields, Some(view)) => {
					em.emit(show(RecordImage::Initial, view));
					em.emit(show(RecordImage::Current, view));
					em.emit(set(match projection {
						Some(pipeline) => WriteOutput::Projection(pipeline),
						None => WriteOutput::Shown(RecordImage::Current),
					}));
				}
				(OutputShape::Default, Some(view)) => {
					let absent = em.label()?;
					em.emit(Instr::Write(WriteInstr::JumpIfNoPostImage {
						target: absent,
					}));
					em.emit(show(RecordImage::Current, view));
					em.emit(set(WriteOutput::Shown(RecordImage::Current)));
					let over = em.label()?;
					em.emit(Instr::Jump {
						target: over,
					});
					em.bind(absent)?;
					em.emit(set(WriteOutput::None));
					em.bind(over)?;
				}
				(
					OutputShape::After
					| OutputShape::Before
					| OutputShape::Diff
					| OutputShape::Default
					| OutputShape::Fields,
					None,
				) => {
					return Err(IrError::Invariant {
						what: "ir_lower::stage: a shown output with no view",
					});
				}
			}
			em.bind(imported)?;
		}
	}
	Ok(())
}

/// What a `RETURN` of fields yields of the images of a record.
enum Returned {
	/// The post-image itself: `RETURN *` alone.
	Image,
	/// The projection `shape` of the post-image, whose columns are `regions`.
	Projection {
		shape: ProjectionShape,
		regions: Vec<RegionId>,
	},
	/// Any other value of the images.
	Value,
}

/// What the region `region`, which the write holds for its `RETURN`, yields.
fn returned(ir: &Ir, region: RegionId) -> Result<Returned, IrError> {
	let yielded = yielded_value(ir, region)?;
	let row = first_arg(ir, region)
		.ok_or(IrError::Stale("ir_lower::stage: a RETURN region takes the image as its row"))?;
	if yielded == row {
		return Ok(Returned::Image);
	}
	let op = producer_of(ir, yielded)?;
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::stage: the op of a RETURN is gone"))?;
	match &data.kind {
		OpKind::Phys(PhysOp::Project(shape)) if data.operands.first() == Some(&row) => {
			match shape.as_ref() {
				ProjectionShape::All => Ok(Returned::Image),
				ProjectionShape::Value
				| ProjectionShape::OmittedValue {
					..
				}
				| ProjectionShape::SortedValue(_)
				| ProjectionShape::Fields {
					..
				}
				| ProjectionShape::Star {
					..
				} => Ok(Returned::Projection {
					shape: shape.as_ref().clone(),
					regions: data.regions.to_vec(),
				}),
			}
		}
		OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_) => Ok(Returned::Value),
	}
}

/// The host pipeline over the post-image of a record that emits what the write's `RETURN` of
/// fields yields of the two images, or none for `RETURN *` alone, which is the image itself.
///
/// The row is the post-image, which `$after`, `$this` and `$value` name too, and `$before` is the
/// parameter the host binds to the pre-image. The image of a record a `DELETE` removes is `NONE`,
/// so its row is no object.
fn return_pipeline(em: &mut Emitter, cx: &mut Stages<'_, '_>) -> Result<Option<u16>, IrError> {
	let run = cx.run;
	let region =
		run.returns.ok_or(IrError::Stale("ir_lower::stage: a RETURN of fields hosts no region"))?;
	let returned = returned(cx.ir, region)?;
	if matches!(returned, Returned::Image) {
		return Ok(None);
	}
	let objects = run.form != WriteForm::Delete;
	cx.host(em, |lowerer| {
		let ir = lowerer.ir;
		let args = lowerer.region_args(region)?;
		let [row, before] = <[_; 2]>::try_from(args).map_err(|_| {
			IrError::Stale(
				"ir_lower::stage: a RETURN region takes the post-image and the pre-image",
			)
		})?;
		lowerer.bind_row(row, objects);
		if !ir.uses(before).is_empty() {
			let param = lowerer.em.param(ParamSpec {
				name: Strand::new("before"),
				guard: None,
			})?;
			let dst = lowerer.em.reg()?;
			lowerer.emit(Instr::LoadParam {
				param,
				dst,
				frame: None,
			});
			lowerer.bind_arg(before, Operand::Reg(dst));
		}
		let out = match &returned {
			Returned::Projection {
				shape,
				regions,
			} => project_image(lowerer, shape, regions, objects)?,
			Returned::Image => {
				return Err(IrError::Invariant {
					what: "ir_lower::stage: a RETURN of the image itself holds no pipeline",
				});
			}
			Returned::Value => {
				let value = lowerer.region_value(region)?;
				let out = lowerer.em.reg()?;
				lowerer.emit(Instr::Load {
					dst: out,
					src: value,
				});
				out
			}
		};
		emit_clause(lowerer, out)
	})
	.map(Some)
}

/// Derives every computed field of the written table onto both images of an observed record, by
/// the one host pipeline the `Observe` instruction runs over them.
///
/// A body that holds a statement, a closure or a call of a user-defined function is lowered for
/// the caller, as [`derive_fields`] lowers it, which refuses one under an `AUTH LIMIT` that
/// narrows the caller.
fn observed(
	em: &mut Emitter,
	cx: &mut Stages<'_, '_>,
	stage: WriteStageKind,
) -> Result<(), IrError> {
	let run = cx.run;
	let fields = run.derived.observed.as_deref().ok_or(IrError::Invariant {
		what: "ir_lower::stage: the computed fields of a table that stores fields are not stated",
	})?;
	if fields.is_empty() {
		return Ok(());
	}
	let regions = run
		.hosted
		.iter()
		.find_map(|(held, hosted)| (*held == stage).then_some(&hosted.derivations[..]))
		.unwrap_or_default();
	let pairs = derivations(fields, regions)?;
	let pipeline = cx.host(em, |lowerer| observed_pipeline(lowerer, &pairs, None))?;
	em.emit(schema(SchemaInstr::Observe {
		pipelines: Arc::from(vec![pipeline]),
	}));
	Ok(())
}

/// Emits the stage that takes a deleted record out of storage and repairs what references it.
///
/// A record no mutation phase changed is left alone. Otherwise the record and its own cache keys
/// are deleted, an edge's adjacency keys on its endpoints are purged, and the cascades run: the
/// edges joined to the record are deleted, then each reference key naming the record is drained,
/// its referencing record repaired as the field that holds the reference directs, and the keys
/// released. The cascade runs for a table that no field references too, because the references
/// a record holds are drained at run time from the keys the database stores.
///
/// The statements a cascade performs on its own behalf are units the compile driver plans
/// ([`UnitRequest::Cascade`]), each requested for the frame it runs in: the edge cascade restores
/// the permission state of the frame the write is lowered for, the delete of a referencing record
/// suppresses enforcement for that record while keeping the writer's for whatever the delete
/// triggers, and a repair reads and updates with permissions off. A unit a cascade reaches
/// through its own records' references is the unit being lowered, which the unit table shares.
fn purge_stage(em: &mut Emitter, cx: &mut Stages<'_, '_>) -> Result<(), IrError> {
	let tables = repair_tables(em, cx)?;
	let edges = cascade_unit(cx, CascadeKind::Edges)?;
	let unchanged = em.label()?;
	jump_if(em, StorageFact::Unmodified, true, unchanged);
	em.emit(storage(StorageInstr::DeleteRecord));
	let vertex = em.label()?;
	jump_if(em, StorageFact::WasEdge, false, vertex);
	em.emit(storage(StorageInstr::PurgePointers));
	em.bind(vertex)?;
	let references = |instr| storage(StorageInstr::References(instr));
	let none = em.label()?;
	let taken = em.label()?;
	let top = em.label()?;
	em.emit(references(ReferenceInstr::PurgeEdges {
		unit: Some(edges),
	}));
	em.emit(references(ReferenceInstr::Drain {
		target: none,
	}));
	em.bind(top)?;
	em.emit(references(ReferenceInstr::Next {
		target: taken,
	}));
	em.emit(references(ReferenceInstr::Repair {
		tables,
	}));
	em.emit(Instr::Jump {
		target: top,
	});
	em.bind(taken)?;
	em.emit(references(ReferenceInstr::Release));
	em.bind(none)?;
	em.bind(unchanged)
}

/// The unit that runs the cascade statement `kind`, for the frame it runs in.
///
/// The edge cascade is requested for the permission state of the actor the write runs on behalf
/// of, the delete of a referencing record suppresses enforcement and keeps the actor's own for
/// what the delete triggers, and a read or an update of a referencing record suppresses both. The
/// actor's state is the frame's, except in the unit of a referencing record's delete, which runs
/// with enforcement suppressed on behalf of the writer ([`LowerCx::actor_suppressed`]).
fn cascade_unit(cx: &mut Stages<'_, '_>, kind: CascadeKind) -> Result<UnitId, IrError> {
	let units = cx.cx.as_deref_mut().ok_or(IrError::Invariant {
		what: "ir_lower::stage: the cascades of a delete are lowered with no unit table",
	})?;
	let writer = units.actor_suppressed();
	let (suppressed, caller) = match kind {
		CascadeKind::Edges => (writer, writer),
		CascadeKind::Delete {
			..
		} => (true, writer),
		CascadeKind::Read {
			..
		}
		| CascadeKind::Unset {
			..
		}
		| CascadeKind::Subtract {
			..
		}
		| CascadeKind::MaterializeWrite {
			..
		}
		| CascadeKind::MaterializeFilter {
			..
		} => (true, true),
	};
	units.unit_for(UnitRequest::Cascade {
		kind,
		suppressed,
		caller,
	})
}

/// The steps that keep the lists of the written table's `MATERIALIZE INTO` fields: for each, the
/// units that add a record to a list, take one out, and judge the clause's `WHERE` condition.
/// `None` for a table with no such field.
fn materialize_steps(
	cx: &mut Stages<'_, '_>,
	run: &StageRun,
) -> Result<Option<Arc<[MaterializeStep]>>, IrError> {
	if run.stored.materialized.is_empty() {
		return Ok(None);
	}
	let table = run.stored.table.clone().ok_or(IrError::Invariant {
		what: "ir_lower::stage: a stated table fact with no table",
	})?;
	let mut steps = Vec::with_capacity(run.stored.materialized.len());
	for materialized in run.stored.materialized.iter() {
		let filter = if materialized.filtered {
			Some(cascade_unit(
				cx,
				CascadeKind::MaterializeFilter {
					table: table.clone(),
					field: materialized.field.clone(),
				},
			)?)
		} else {
			None
		};
		steps.push(MaterializeStep {
			field: materialized.field.clone(),
			table: materialized.table.clone(),
			filter,
			write: cascade_unit(
				cx,
				CascadeKind::MaterializeWrite {
					table: materialized.table.clone(),
					dest: materialized.dest.clone(),
				},
			)?,
		});
	}
	Ok(Some(steps.into()))
}

/// The repairs of every table that defines a `REFERENCE` field which can name a record of the
/// written table, each field by the strategy its `ON DELETE` clause directs.
///
/// A table no field references has no repair. A write that names no one table answers `NotYet`
/// from the stage that needs the one table, and one bound before a catalog change answers
/// [`IrError::LateBind`].
fn repair_tables(
	em: &mut Emitter,
	cx: &mut Stages<'_, '_>,
) -> Result<Arc<[TableRepairs]>, IrError> {
	let run = cx.run;
	let set = match run.stored.referenced {
		Stored::Nothing => return Ok(Arc::from(Vec::new())),
		Stored::Declared => &run.stored.repair_set,
		kind @ (Stored::Dynamic | Stored::LateBound) => {
			no_clauses(kind, "the references to the written table")?;
			return Err(IrError::Invariant {
				what: "ir_lower::stage: a table whose references are not stated passed the check for one",
			});
		}
	};
	if set.unlowerable {
		return Err(IrError::Invariant {
			what: "ir_lower::stage: a THEN repair with no logical form, or one that holds a write no operator runs",
		});
	}
	let mut regions = run.repairs.iter().copied();
	let mut repairs = Vec::with_capacity(set.tables.len());
	for table in set.tables.iter() {
		let mut declared = Vec::with_capacity(table.fields.len());
		for field in table.fields.iter() {
			let strategy = match field.strategy {
				RepairKind::Ignore => RepairStrategy::Ignore,
				RepairKind::Reject => RepairStrategy::Reject,
				RepairKind::Cascade => RepairStrategy::Cascade {
					unit: cascade_unit(
						cx,
						CascadeKind::Delete {
							table: table.table.clone(),
						},
					)?,
				},
				RepairKind::Unset => RepairStrategy::Unset {
					field: field.name.clone(),
					read: cascade_unit(
						cx,
						CascadeKind::Read {
							table: table.table.clone(),
						},
					)?,
					unset: cascade_unit(
						cx,
						CascadeKind::Unset {
							table: table.table.clone(),
							field: field.name.clone(),
						},
					)?,
					subtract: cascade_unit(
						cx,
						CascadeKind::Subtract {
							table: table.table.clone(),
							field: field.name.clone(),
						},
					)?,
				},
				RepairKind::Custom => {
					let region = regions.next().ok_or(IrError::Invariant {
						what: "ir_lower::stage: a THEN repair the references state has no region",
					})?;
					RepairStrategy::Custom {
						clause: cx.host(em, |lowerer| repair_pipeline(lowerer, region))?,
					}
				}
			};
			declared.push((field.name.to_sql(), strategy));
		}
		repairs.push(TableRepairs {
			table: table.table.clone(),
			fields: declared,
		});
	}
	if regions.next().is_some() {
		return Err(IrError::Stale("ir_lower::stage: a region no repair reads"));
	}
	Ok(repairs.into())
}

/// The host pipeline over the id of a referencing record that emits the value of the `THEN`
/// region `region`, in the privileged frame a purge runs its repairs in: permissions off and
/// writes allowed. The record id is the row, and `$reference`, the purged record, is the parameter
/// the run binds, read where the expression reads it.
fn repair_pipeline(lowerer: &mut Lowerer<'_>, region: RegionId) -> Result<Pipeline, IrError> {
	let ir = lowerer.ir;
	let container = region_frame(ir, region)?;
	let held =
		ir.frame(container).ok_or(IrError::Stale("ir_lower::stage: the frame of a THEN repair"))?;
	if held.eval != EvalFrame::mutable_permission(None) {
		return Err(IrError::Invariant {
			what: "ir_lower::stage: a THEN repair outside the frame of a purge",
		});
	}
	let args = lowerer.region_args(region)?;
	let [row, reference] = <[_; 2]>::try_from(args).map_err(|_| {
		IrError::Stale("ir_lower::stage: a THEN repair takes the referencing record and $reference")
	})?;
	lowerer.bind_row(row, false);
	if !ir.uses(reference).is_empty() {
		let param = lowerer.em.param(ParamSpec {
			name: Strand::new("reference"),
			guard: None,
		})?;
		let dst = lowerer.em.reg()?;
		lowerer.emit(Instr::LoadParam {
			param,
			dst,
			frame: None,
		});
		lowerer.bind_arg(reference, Operand::Reg(dst));
	}
	lowerer.in_host_frame(container, |lowerer| {
		let value = lowerer.region_value(region)?;
		let out = lowerer.em.reg()?;
		lowerer.emit(Instr::Load {
			dst: out,
			src: value,
		});
		emit_clause(lowerer, out)
	})
}

/// Emits the instruction that runs the events of the written table for `action`, as the unit the
/// compile driver plans for them.
///
/// A table that stores no event and a statement that cannot change the catalog run none for any
/// record, so no instruction stands for them. Otherwise the unit is requested with the frame the
/// write is lowered for, as the run admits it against the frame the write runs in, even for a
/// table that holds no event when the statement is lowered: a statement that may define one runs
/// the unit over the events the table holds when each record is written.
fn events_stage(
	em: &mut Emitter,
	cx: &mut Stages<'_, '_>,
	action: crate::doc::Action,
) -> Result<(), IrError> {
	let run = cx.run;
	match run.stored.events {
		Stored::Nothing if run.stored.catalog_fixed => return Ok(()),
		Stored::Nothing | Stored::Declared => {}
		Stored::Dynamic => {
			return Err(not_yet("ir_lower::stage: a stage of a write that names no one table"));
		}
		Stored::LateBound => {
			return Err(IrError::LateBind {
				what: "ir_lower::stage: the events of a table read before a catalog change",
			});
		}
	}
	let table = run.stored.table.clone().ok_or(IrError::Invariant {
		what: "ir_lower::stage: a stated table fact with no table",
	})?;
	let nesting = run.nesting;
	let units = cx.cx.as_deref_mut().ok_or(IrError::Invariant {
		what: "ir_lower::stage: the events of a write are lowered with no unit table",
	})?;
	let depth = units.depth() + units.nesting().within(nesting).depth();
	let unit = units.unit_for(UnitRequest::Events {
		table,
		action,
		frame: units.frame().clone(),
	})?;
	em.emit(Instr::Write(WriteInstr::Observe(ObserveInstr::Events {
		unit,
		depth,
	})));
	Ok(())
}

/// Whether a record the write creates is left in the transaction's record cache.
///
/// A statement that returns nothing and whose table runs no event has no clause that reads the
/// records it creates while it runs, so a bulk create pays no cache insert per record. Every other
/// create keeps the record, which a later read in the statement may take from the cache. A record
/// left out is read from storage, which holds the transaction's writes, so the choice never
/// changes what a read returns.
fn keeps_created(run: &StageRun) -> bool {
	!(matches!(run.output, OutputShape::None)
		&& matches!(run.stored.events, Stored::Nothing)
		&& run.stored.catalog_fixed)
}

/// The refusal for a stage whose clause the lowering has no form for.
fn not_yet(what: &'static str) -> IrError {
	IrError::NotYet {
		what,
	}
}

/// Whether the write states what its table's field declarations say: a write that names no one
/// table answers [`IrError::NotYet`], because the stages run in the unit planned for each table
/// it reaches, and one bound before a catalog change answers [`IrError::LateBind`].
fn stated(run: &StageRun) -> Result<ImageView, IrError> {
	match run.image.view {
		ImageView::Dynamic => {
			Err(not_yet("ir_lower::stage: a stage of a write that names no one table"))
		}
		ImageView::LateBound => Err(IrError::LateBind {
			what: "ir_lower::stage: the fields of a table read before a catalog change",
		}),
		view @ (ImageView::Plain | ImageView::Cut | ImageView::Derived) => Ok(view),
	}
}

/// Binds the block arguments of the clause region `region`: the first is the snapshot the clause
/// is evaluated over, the row of the pipeline being lowered, and an `ON DUPLICATE KEY UPDATE`
/// takes `$input` second, the parameter the run binds to the value the statement supplied. The
/// snapshot is no record of the table's own, so it is not an object row.
fn bind_snapshot(lowerer: &mut Lowerer<'_>, region: RegionId) -> Result<(), IrError> {
	let ir = lowerer.ir;
	let duplicate = ir
		.region(region)
		.is_some_and(|held| held.kind == RegionKind::Clause(ClauseRole::DuplicateUpdate));
	let args = lowerer.region_args(region)?;
	let (row, input) = match (args.as_slice(), duplicate) {
		([row], _) => (*row, None),
		([row, input], true) => (*row, Some(*input)),
		_ => return Err(IrError::Stale("ir_lower::stage: a clause takes the snapshot as its row")),
	};
	lowerer.bind_row(row, false);
	if let Some(input) = input.filter(|input| !ir.uses(*input).is_empty()) {
		let param = lowerer.em.param(ParamSpec {
			name: Strand::new("input"),
			guard: None,
		})?;
		let dst = lowerer.em.reg()?;
		lowerer.emit(Instr::LoadParam {
			param,
			dst,
			frame: None,
		});
		lowerer.bind_arg(input, Operand::Reg(dst));
	}
	Ok(())
}

/// Ends the pipeline of a clause as one that emits the value in `out` and ends its row.
pub(super) fn emit_clause(lowerer: &mut Lowerer<'_>, out: Reg) -> Result<Pipeline, IrError> {
	lowerer.emit(Instr::Emit {
		src: out,
	});
	lowerer.emit(Instr::EndRow);
	lowerer.em.finish_pipeline(Source::Once)
}

/// The host pipeline over a copy of an image that emits the verdict of the permission predicate
/// region `region`, judged in the gate frame of `guard`: the frame its ops carry, and the
/// permission frame of the program that evaluates under its `AUTH LIMIT`. The computed fields
/// `fields` are derived onto the copy first, each landing before the next is evaluated.
///
/// The image a gate judges need not be a record, so it is no object row. The frame the ops carry is
/// the guard's own: a read guard is a permission frame and a write guard a mutable one.
pub(super) fn gate_pipeline(
	lowerer: &mut Lowerer<'_>,
	region: RegionId,
	guard: GuardKind,
	fields: &[(&DerivedField, RegionId)],
) -> Result<Pipeline, IrError> {
	let ir = lowerer.ir;
	let container = region_frame(ir, region)?;
	let held = ir
		.frame(container)
		.ok_or(IrError::Stale("ir_lower::stage: the frame of a permission predicate"))?;
	let agrees = matches!(
		(held.eval.mode, guard),
		(EvalMode::Permission, GuardKind::Read) | (EvalMode::MutablePermission, GuardKind::Write)
	);
	if !agrees {
		return Err(IrError::Invariant {
			what: "ir_lower::stage: a permission predicate outside the frame of its guard",
		});
	}
	let program = lowerer.em.permission_frame(PermissionSpec {
		mode: FrameMode::Gate(guard),
		auth: held.eval.auth.clone(),
		column: false,
	})?;
	let args = lowerer.region_args(region)?;
	let [_, row] = <[_; 2]>::try_from(args).map_err(|_| {
		IrError::Stale("ir_lower::stage: a permission predicate takes the timestamp and the image")
	})?;
	lowerer.bind_row(row, false);
	if !fields.is_empty() {
		derive_fields(lowerer, fields, Derive::Gate, None)?;
	}
	lowerer.in_frame(container, program, |lowerer| {
		let verdict = lowerer.region_value(region)?;
		let out = lowerer.em.reg()?;
		lowerer.emit(Instr::Load {
			dst: out,
			src: verdict,
		});
		emit_clause(lowerer, out)
	})
}

/// Where computed fields are derived, which decides the frame each body runs in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Derive {
	/// Onto the copy of an image a permission predicate judges: in the gate's derived frame, which
	/// does not check the reads the body makes and refuses its writes. The row is an image that
	/// need not be a record.
	Gate,
	/// Onto both images of an observed record: in the computed frame, which checks the reads the
	/// body makes as the definer and refuses its writes. The row is the record's image.
	Observed,
}

impl Derive {
	/// The mode the container frame of a body of this kind carries, and the mode of the frame the
	/// program evaluates it in.
	fn frames(self) -> (EvalMode, FrameMode) {
		match self {
			Self::Gate => (EvalMode::Permission, FrameMode::Derived),
			Self::Observed => (EvalMode::Computed, FrameMode::Computed),
		}
	}
}

/// Derives each field of `fields` onto the row of the pipeline being lowered, in order: the body
/// of its region evaluated against the row with the fields derived before it set, in a frame of
/// `how` under the field's `AUTH LIMIT`, coerced to the field's kind, and landed on the row before
/// the next body is evaluated. A body that reads a computed field reads the row's value under its
/// name, which a derivation before it set, or the row's stored fields where it set none.
///
/// A body under an `AUTH LIMIT` that narrows the caller runs in a frame of the narrowed principal:
/// a statement or a call of a user-defined function it holds is a unit planned for that principal
/// (`region_limit`, the call's request). A closure it makes answers `NotYet`
/// ([`refuse_narrowed`]).
///
/// A self-dereference reads the row's stored fields unless the row carries a key under one of the
/// names the derivation computes: those are the fields derived, or `names` when it is stated, which
/// is every computed field of the table for a field a later clause reads.
pub(super) fn derive_fields(
	lowerer: &mut Lowerer<'_>,
	fields: &[(&DerivedField, RegionId)],
	how: Derive,
	names: Option<&[Strand]>,
) -> Result<(), IrError> {
	let ir = lowerer.ir;
	let (eval, mode) = how.frames();
	let names = match names {
		Some(names) => names.to_vec(),
		None => fields.iter().map(|(field, _)| field.name.clone()).collect(),
	};
	let names = lowerer.em.computed_names(names)?;
	let id = lowerer.em.field("id")?;
	lowerer.emit(Instr::BeginComputing {
		id,
		doc: None,
		names,
	});
	for (field, region) in fields {
		let container = region_frame(ir, *region)?;
		let held = ir
			.frame(container)
			.ok_or(IrError::Stale("ir_lower::stage: the frame of a computed field"))?;
		if held.eval.mode != eval {
			return Err(IrError::Invariant {
				what: "ir_lower::stage: a computed field derived outside the frame of its stage",
			});
		}
		refuse_narrowed(ir, *region, field.caller_bound)?;
		let program = lowerer.em.permission_frame(PermissionSpec {
			mode,
			auth: held.eval.auth.clone(),
			column: false,
		})?;
		let args = lowerer.region_args(*region)?;
		let [_, row] = <[_; 2]>::try_from(args)
			.map_err(|_| IrError::Stale("ir_lower::stage: a computed field takes the image"))?;
		lowerer.bind_row(row, how == Derive::Observed);
		let value =
			lowerer.in_frame(container, program, |lowerer| lowerer.region_value(*region))?;
		let reg = lowerer.em.reg()?;
		lowerer.emit(Instr::Load {
			dst: reg,
			src: value,
		});
		let name = lowerer.em.field(field.name.as_str())?;
		if let Some(kind) = &field.kind {
			let kind = lowerer.em.kind(kind.clone())?;
			lowerer.emit(Instr::CoerceField {
				reg,
				kind,
				field: name,
			});
		}
		let (sets, cuts) = (lowerer.em.reg()?, lowerer.em.reg()?);
		lowerer.emit(Instr::NewObject {
			dst: sets,
		});
		lowerer.emit(Instr::NewObject {
			dst: cuts,
		});
		let path = lowerer.em.path(FieldPath::field(field.name.as_str()))?;
		lowerer.emit(Instr::SetField {
			object: sets,
			path,
			value: Operand::Reg(reg),
		});
		lowerer.emit(Instr::LandRow {
			sets,
			cuts,
		});
	}
	Ok(())
}

/// The host pipeline that derives `fields`, every computed field of the written table, onto the
/// image it is handed and answers the image.
fn observed_pipeline(
	lowerer: &mut Lowerer<'_>,
	fields: &[(&DerivedField, RegionId)],
	names: Option<&[Strand]>,
) -> Result<Pipeline, IrError> {
	derive_fields(lowerer, fields, Derive::Observed, names)?;
	lowerer.emit(Instr::Emit {
		src: ROW,
	});
	lowerer.emit(Instr::EndRow);
	lowerer.em.finish_pipeline(Source::Once)
}

/// The `FieldFrame(Computed)` regions `regions` that derive the fields `fields` of a predicate,
/// taken from the front of `regions`.
fn take_derivations<'r, 'f>(
	fields: &'f [DerivedField],
	regions: &mut std::slice::Iter<'r, RegionId>,
) -> Result<Vec<(&'f DerivedField, RegionId)>, IrError> {
	fields
		.iter()
		.map(|field| {
			regions.next().map(|region| (field, *region)).ok_or(IrError::Invariant {
				what: "ir_lower::stage: a stage hosts a region for each computed field it derives",
			})
		})
		.collect()
}

/// The facts on which `StoreIndexData` leaves the record's entries alone, because the write leaves
/// the record's storage alone: a record no mutation phase changed, unless the session forces index
/// maintenance, and a record of a `DROP` table in a write that stores through `StoreRecordData`,
/// which writes nothing for one. Every other write changes storage whatever the table's `DROP`
/// clause says: a delete's purge removes the stored record, and a view's maintenance writes and
/// removes its rows. An index holds an entry for exactly the records storage holds, so there the
/// entries follow the record.
fn index_skips(run: &StageRun) -> &'static [StorageFact] {
	if run.stages.contains(&WriteStageKind::StoreRecordData) {
		&[StorageFact::Unmaintained, StorageFact::TableDropped]
	} else {
		&[StorageFact::Unmaintained]
	}
}

/// Brings every index of the written table in line with the record's two images, one instruction
/// per index in the catalog's order: a B-tree or `UNIQUE` index's entries from its columns, written
/// by the host from the values a host pipeline answers for an image, and any other kind through its
/// own implementation from the same columns and, for a `COUNT` index, its guard. The record is left
/// alone where [`index_skips`] says storage is.
///
/// The program names the definitions it was lowered from, and a record whose table holds other
/// definitions by the time it is written fails the statement. The pipelines of every index compile
/// before anything is emitted, so a column the lowering has no form for refuses the statement with
/// the body as it was.
fn store_indexes(em: &mut Emitter, cx: &mut Stages<'_, '_>) -> Result<(), IrError> {
	let run = cx.run;
	let set = &run.stored.index_set;
	if set.unlowerable {
		return Err(IrError::Invariant {
			what: "ir_lower::stage: an index column with no logical form, or one that holds a write no operator runs",
		});
	}
	let defs = set.defs.definitions();
	if defs.is_empty() {
		return Err(IrError::Invariant {
			what: "ir_lower::stage: a table that declares indexes states none",
		});
	}
	let mut regions = run.indexes.iter().copied();
	let mut per_index = Vec::with_capacity(defs.len());
	for index in defs.iter() {
		if index.prepare_remove {
			per_index.push(IndexPlan::Removed);
			continue;
		}
		let columns = (0..index.cols.len())
			.map(|_| next_index_region(&mut regions))
			.collect::<Result<Vec<_>, _>>()?;
		let columns = cx.host(em, |lowerer| index_columns_pipeline(lowerer, &columns))?;
		per_index.push(match NativeIndexKind::of(&index.index) {
			None => IndexPlan::Entries {
				columns,
			},
			Some(kind) => IndexPlan::Native {
				kind,
				columns,
				guard: match index.count_cond {
					Some(_) => {
						let region = next_index_region(&mut regions)?;
						Some(cx.host(em, |lowerer| index_guard_pipeline(lowerer, region))?)
					}
					None => None,
				},
			},
		});
	}
	if regions.next().is_some() {
		return Err(IrError::Invariant {
			what: "ir_lower::stage: the stage hosts an index region its indexes do not state",
		});
	}
	let lowered = Arc::new(LoweredIndexes::new(Arc::clone(defs), per_index));
	let end = em.label()?;
	for fact in index_skips(run) {
		jump_if(em, *fact, true, end);
	}
	em.emit(storage(StorageInstr::CheckIndexes {
		lowered: Arc::clone(&lowered),
	}));
	for (position, plan) in lowered.per_index.iter().enumerate() {
		let position = u16::try_from(position).map_err(|_| IrError::Invariant {
			what: "ir_lower::stage: a table has more indexes than a program names",
		})?;
		match plan {
			IndexPlan::Removed => {}
			IndexPlan::Entries {
				..
			} => {
				em.emit(storage(StorageInstr::IndexEntries {
					lowered: Arc::clone(&lowered),
					position,
				}));
			}
			IndexPlan::Native {
				..
			} => {
				em.emit(storage(StorageInstr::IndexNative {
					lowered: Arc::clone(&lowered),
					position,
				}));
			}
		}
	}
	em.bind(end)
}

/// The next region of the columns and guards of an index, or the invariant break of an index that
/// states a column no region holds.
fn next_index_region(regions: &mut impl Iterator<Item = RegionId>) -> Result<RegionId, IrError> {
	regions.next().ok_or(IrError::Invariant {
		what: "ir_lower::stage: a column the indexes state has no region",
	})
}

/// Binds the one block argument of the index column region `region`, which is the image the index
/// is evaluated over, as the row of the pipeline being lowered, and enters the frame an index
/// evaluates its columns in: the statement's own context, privileged, holding none of the caller's
/// bindings. The region is bound in the frame of the write stage that hosts it, whatever clause
/// encloses the write, and runs as an index entry under no `AUTH LIMIT` whatever that frame
/// states. `objects` is whether the image is a record whose fields are read from the row.
fn index_value(
	lowerer: &mut Lowerer<'_>,
	region: RegionId,
	objects: bool,
) -> Result<Operand, IrError> {
	let ir = lowerer.ir;
	let container = region_frame(ir, region)?;
	let owner = ir
		.region(region)
		.and_then(|held| held.parent)
		.and_then(|owner| ir.op(owner))
		.ok_or(IrError::Stale("ir_lower::stage: the stage that hosts an index column"))?;
	if container != owner.frame {
		return Err(IrError::Invariant {
			what: "ir_lower::stage: an index column outside the frame of the statement",
		});
	}
	let args = lowerer.region_args(region)?;
	let [row] = <[_; 1]>::try_from(args).map_err(|_| {
		IrError::Stale("ir_lower::stage: an index column takes the image as its row")
	})?;
	lowerer.bind_row(row, objects);
	let program = lowerer.em.permission_frame(PermissionSpec {
		mode: FrameMode::IndexEntry,
		auth: None,
		column: false,
	})?;
	lowerer.in_frame(container, program, |lowerer| lowerer.region_value(region))
}

/// The host pipeline over an image that emits the array of the values the columns whose regions are
/// `regions` hold for it, in the definition's order. The image is the record a self-dereference in
/// a column answers from.
fn index_columns_pipeline(
	lowerer: &mut Lowerer<'_>,
	regions: &[RegionId],
) -> Result<Pipeline, IrError> {
	let names = lowerer.em.computed_names(Vec::new())?;
	let id = lowerer.em.field("id")?;
	lowerer.emit(Instr::BeginComputing {
		id,
		doc: Some(ROW),
		names,
	});
	let values = lowerer.em.reg()?;
	lowerer.emit(Instr::NewArray {
		dst: values,
	});
	for region in regions {
		let value = index_value(lowerer, *region, true)?;
		lowerer.emit(Instr::ArrayPush {
			array: values,
			value,
		});
	}
	emit_clause(lowerer, values)
}

/// The host pipeline over an image that emits the value of the guard of a `COUNT` index, whose
/// region is `region`. The image need not exist, so it is no object row: a field the guard names is
/// read by the expression that names it.
fn index_guard_pipeline(lowerer: &mut Lowerer<'_>, region: RegionId) -> Result<Pipeline, IrError> {
	let value = index_value(lowerer, region, false)?;
	let out = lowerer.em.reg()?;
	lowerer.emit(Instr::Load {
		dst: out,
		src: value,
	});
	emit_clause(lowerer, out)
}

/// Emits `ProcessTableViews` for `action`: the instruction that maintains the views the written
/// table feeds, lowered from the view clauses the stage hosts.
///
/// A table that feeds no view holds the instruction with no view to maintain. The views are
/// lowered once for the write, whichever stage of its orders maintains them.
fn views_stage(
	em: &mut Emitter,
	cx: &mut Stages<'_, '_>,
	action: crate::doc::Action,
) -> Result<(), IrError> {
	let lowered = match cx.run.stored.views {
		Stored::Nothing => Arc::new(LoweredViews {
			definitions: Arc::from(Vec::new()),
			views: Vec::new(),
		}),
		Stored::Declared => match &cx.views {
			Some(lowered) => Arc::clone(lowered),
			None => {
				let lowered = lower_views(em, cx)?;
				cx.views = Some(Arc::clone(&lowered));
				lowered
			}
		},
		kind @ (Stored::Dynamic | Stored::LateBound) => {
			no_clauses(kind, "the views of the written table")?;
			return Err(IrError::Invariant {
				what: "ir_lower::stage: a table whose views are not stated passed the check for one",
			});
		}
	};
	em.emit(Instr::Write(WriteInstr::Observe(ObserveInstr::Views {
		action,
		lowered,
	})));
	Ok(())
}

/// Whether a materialized view's `fields` set the row's `id`, so that the row's key is the one
/// the projection gives rather than the source record's: a field aliased `id` or a path within it
/// (`... AS id`, `... AS id.k`), or an unaliased path into the record's `id` (`id.k`, `id[0]`),
/// which a projection writes back at `id`. A bare `id` is the source record's own.
fn projects_id(fields: &crate::expr::Fields) -> bool {
	let crate::expr::Fields::Select(fields) = fields else {
		return false;
	};
	let names_id = |idiom: &crate::expr::Idiom| idiom.0.first() == Some(&crate::expr::paths::ID[0]);
	fields.iter().any(|field| match field {
		crate::expr::Field::Single(selector) => match (&selector.alias, &selector.expr) {
			(Some(alias), _) => names_id(alias),
			(None, crate::expr::Expr::Idiom(idiom)) => idiom.0.len() > 1 && names_id(idiom),
			(None, _) => false,
		},
		crate::expr::Field::All => false,
	})
}

/// The views the written table feeds, each lowered as its definition and clause regions state:
/// the clauses as host pipelines of the program, and the order of the view table's rows and the
/// recalculation of an aggregated view's extremes as units of it.
fn lower_views(em: &mut Emitter, cx: &mut Stages<'_, '_>) -> Result<Arc<LoweredViews>, IrError> {
	let run = cx.run;
	let set = &run.stored.view_set;
	if set.unlowerable {
		return Err(IrError::Invariant {
			what: "ir_lower::stage: a view clause with no logical form, one that holds a write no operator runs, or a VALUE selector on an aggregated view",
		});
	}
	let defs = set.defs.definitions();
	if defs.is_empty() {
		return Err(IrError::Invariant {
			what: "ir_lower::stage: a table that feeds views states none",
		});
	}
	let mut regions = run.views.iter().copied();
	let mut views = Vec::with_capacity(defs.len());
	for definition in defs.iter() {
		let view = definition.view.as_ref().ok_or(IrError::Invariant {
			what: "ir_lower::stage: a view table is listed among the views of a table and holds no view",
		})?;
		views.push(match view {
			crate::catalog::ViewDefinition::Materialized {
				fields,
				condition,
				..
			} => {
				let rows = view_rows(cx, definition)?;
				let condition = match condition {
					Some(_) => Some(view_clause(em, cx, &mut regions)?),
					None => None,
				};
				let region = next_view_region(&mut regions)?;
				let projection = cx.host(em, |lowerer| view_projection(lowerer, region))?;
				LoweredView::Materialized {
					condition,
					projection: ViewClause {
						object: projection,
						any: projection,
					},
					keyed: projects_id(fields),
					rows,
					reads_data: view_reads_data(definition, cx.registry),
				}
			}
			crate::catalog::ViewDefinition::Aggregated {
				analysis,
				condition,
				..
			} => {
				let rows = view_rows(cx, definition)?;
				let condition = match condition {
					Some(_) => Some(view_clause(em, cx, &mut regions)?),
					None => None,
				};
				let mut group = Vec::with_capacity(analysis.group_expressions.len());
				for _ in &analysis.group_expressions {
					group.push(view_clause(em, cx, &mut regions)?);
				}
				let mut arguments = Vec::with_capacity(analysis.aggregate_arguments.len());
				for _ in &analysis.aggregate_arguments {
					arguments.push(view_clause(em, cx, &mut regions)?);
				}
				let crate::catalog::aggregation::AggregateFields::Fields(items) = &analysis.fields
				else {
					return Err(IrError::Invariant {
						what: "ir_lower::stage: a VALUE selector on an aggregated view",
					});
				};
				let mut fields = Vec::with_capacity(items.len());
				for (name, _) in items {
					let pipeline = view_clause(em, cx, &mut regions)?;
					let place = view_place(em, cx, name, &mut regions)?;
					fields.push((place, pipeline));
				}
				let extremes = extremes_of(analysis);
				let recalc = match extremes.is_empty() {
					true => None,
					false => Some(Recalc {
						unit: view_unit(
							cx,
							UnitRequest::ViewRecalc {
								view: definition.name.clone(),
							},
						)?,
						extremes,
					}),
				};
				LoweredView::Aggregated {
					condition,
					group,
					arguments,
					fields,
					recalc,
					rows,
					reads_data: view_reads_data(definition, cx.registry),
				}
			}
			crate::catalog::ViewDefinition::Select {
				..
			} => LoweredView::Unmaintained,
		});
	}
	if regions.next().is_some() {
		return Err(IrError::Invariant {
			what: "ir_lower::stage: the stage hosts a view region its views do not state",
		});
	}
	Ok(Arc::new(LoweredViews {
		definitions: Arc::clone(defs),
		views,
	}))
}

/// The slot of the unit table that holds the unit `request` names, for the views of a write that
/// is lowered with a unit table.
fn view_unit(cx: &mut Stages<'_, '_>, request: UnitRequest) -> Result<UnitId, IrError> {
	cx.cx
		.as_deref_mut()
		.ok_or(IrError::Invariant {
			what: "ir_lower::stage: the views of a write are lowered with no unit table",
		})?
		.unit_for(request)
}

/// The order of the rows of the view table `definition`, a unit of the program.
fn view_rows(
	cx: &mut Stages<'_, '_>,
	definition: &crate::catalog::TableDefinition,
) -> Result<ViewRows, IrError> {
	Ok(ViewRows {
		unit: view_unit(
			cx,
			UnitRequest::ViewRows {
				table: definition.name.clone(),
			},
		)?,
	})
}

/// The next clause region of the views, or the invariant break of a view that states a clause no
/// region holds.
fn next_view_region(regions: &mut impl Iterator<Item = RegionId>) -> Result<RegionId, IrError> {
	regions.next().ok_or(IrError::Invariant {
		what: "ir_lower::stage: a clause the views state has no region",
	})
}

/// The host pipelines of the next clause region of the views: one over an image that is an object,
/// whose fields the pipeline reads itself, and one over any value, which the image of a record
/// that is not there is.
fn view_clause(
	em: &mut Emitter,
	cx: &mut Stages<'_, '_>,
	regions: &mut impl Iterator<Item = RegionId>,
) -> Result<ViewClause, IrError> {
	let region = next_view_region(regions)?;
	let object = cx.host(em, |lowerer| view_expression(lowerer, region, true))?;
	let any = cx.host(em, |lowerer| view_expression(lowerer, region, false))?;
	Ok(ViewClause {
		object,
		any,
	})
}

/// The container frame the ops of the view clause region `region` carry, which is the frame of a
/// view. Answers [`IrError::Invariant`] for a region under any other frame.
fn view_frame_of(lowerer: &Lowerer<'_>, region: RegionId) -> Result<FrameId, IrError> {
	let ir = lowerer.ir;
	let container = region_frame(ir, region)?;
	let held =
		ir.frame(container).ok_or(IrError::Stale("ir_lower::stage: the frame of a view clause"))?;
	if held.eval != EvalFrame::mutable_permission(None) {
		return Err(IrError::Invariant {
			what: "ir_lower::stage: a view clause outside the frame of a view",
		});
	}
	Ok(container)
}

/// The one block argument of the view clause region `region`.
fn view_image(
	lowerer: &Lowerer<'_>,
	region: RegionId,
) -> Result<crate::exec::ir::iface::ids::ValueId, IrError> {
	let args = lowerer.region_args(region)?;
	let [row] = <[_; 1]>::try_from(args)
		.map_err(|_| IrError::Stale("ir_lower::stage: a view clause takes the image as its row"))?;
	Ok(row)
}

/// The host pipeline over an image that emits the value of the view clause region `region`, in the
/// privileged frame the clauses of a view run in. `objects` is whether the image is a record whose
/// fields are read from the row.
fn view_expression(
	lowerer: &mut Lowerer<'_>,
	region: RegionId,
	objects: bool,
) -> Result<Pipeline, IrError> {
	let container = view_frame_of(lowerer, region)?;
	let row = view_image(lowerer, region)?;
	lowerer.bind_row(row, objects);
	lowerer.in_host_frame(container, |lowerer| {
		let value = lowerer.region_value(region)?;
		let out = lowerer.em.reg()?;
		lowerer.emit(Instr::Load {
			dst: out,
			src: value,
		});
		emit_clause(lowerer, out)
	})
}

/// The host pipeline over an image that emits the row a materialized view holds for it: the view's
/// fields projected from the image, which is any value because the image of a record that is not
/// there is `NONE`. The projection is the one the view clause region `region` yields.
fn view_projection(lowerer: &mut Lowerer<'_>, region: RegionId) -> Result<Pipeline, IrError> {
	let container = view_frame_of(lowerer, region)?;
	let row = view_image(lowerer, region)?;
	let ir = lowerer.ir;
	let returned = returned(ir, region)?;
	lowerer.bind_row(row, false);
	lowerer.in_host_frame(container, |lowerer| {
		let out = match &returned {
			Returned::Image => ROW,
			Returned::Projection {
				shape,
				regions,
			} => project_image(lowerer, shape, regions, false)?,
			Returned::Value => {
				let value = lowerer.region_value(region)?;
				let out = lowerer.em.reg()?;
				lowerer.emit(Instr::Load {
					dst: out,
					src: value,
				});
				out
			}
		};
		emit_clause(lowerer, out)
	})
}

/// The steps the place `name` is walked by, the expressions it embeds each a host pipeline of the
/// program over the next view clause region of `regions`: a subscript is evaluated with no record
/// in scope, and a filter judges one element of the array it is applied to. A part no place
/// addresses is a step that stops the walk.
fn view_place(
	em: &mut Emitter,
	cx: &mut Stages<'_, '_>,
	name: &Idiom,
	regions: &mut impl Iterator<Item = RegionId>,
) -> Result<Vec<PlaceStepSpec>, IrError> {
	use surrealdb_types::ToSql as _;

	let mut steps = Vec::with_capacity(name.0.len());
	for part in &name.0 {
		steps.push(match part {
			Part::Field(field) => PlaceStepSpec::Field(field.as_str().to_owned()),
			Part::Lookup(lookup) => PlaceStepSpec::Lookup(lookup.to_sql()),
			Part::Value(_) => {
				let region = next_view_region(regions)?;
				PlaceStepSpec::Subscript {
					expr: cx.host(em, |lowerer| view_subscript(lowerer, region))?,
					literal_index: part.as_old_index().is_some(),
				}
			}
			Part::Where(_) => {
				let region = next_view_region(regions)?;
				PlaceStepSpec::Where(cx.host(em, |lowerer| view_filter(lowerer, region))?)
			}
			Part::First => PlaceStepSpec::First,
			Part::Last => PlaceStepSpec::Last,
			Part::All => PlaceStepSpec::All,
			Part::Flatten
			| Part::Start(_)
			| Part::Method(_, _)
			| Part::Destructure(_)
			| Part::Optional
			| Part::Recurse(_, _, _)
			| Part::Doc
			| Part::RepeatRecurse => PlaceStepSpec::Unaddressed,
		});
	}
	Ok(steps)
}

/// The host pipeline that emits the value of the subscript region `region` once, with no record
/// in scope: the image the region receives holds `NONE`.
fn view_subscript(lowerer: &mut Lowerer<'_>, region: RegionId) -> Result<Pipeline, IrError> {
	let container = view_frame_of(lowerer, region)?;
	let row = view_image(lowerer, region)?;
	let none = lowerer.em.constant(crate::val::Value::None)?;
	lowerer.bind_arg(row, Operand::Const(none));
	lowerer.in_host_frame(container, |lowerer| {
		let value = lowerer.region_value(region)?;
		lowerer.emit_value_pipeline(value)
	})
}

/// The host pipeline that emits what the filter region `region` yields for one element of the
/// array the place is applied to: the pipeline's row is the element, which is any value.
fn view_filter(lowerer: &mut Lowerer<'_>, region: RegionId) -> Result<Pipeline, IrError> {
	let container = view_frame_of(lowerer, region)?;
	let element = view_image(lowerer, region)?;
	lowerer.bind_row(element, false);
	lowerer.in_host_frame(container, |lowerer| {
		let verdict = lowerer.region_value(region)?;
		lowerer.emit_value_pipeline(verdict)
	})
}

/// The next region of a kind a field's clause states, or the invariant break of a schema that
/// states a clause no region holds.
fn next_region(regions: &mut std::slice::Iter<'_, RegionId>) -> Result<RegionId, IrError> {
	regions.next().copied().ok_or(IrError::Invariant {
		what: "ir_lower::stage: a clause the field schema states has no region",
	})
}

/// One step of the field schema as its host pipelines compile, before any instruction is emitted.
enum Compiled {
	/// A stored field: its `DEFAULT`, `VALUE` and permissions as pipelines, and whether it has an
	/// `ASSERT`.
	Field {
		index: u16,
		default: Option<u16>,
		value: Option<u16>,
		asserted: bool,
		write: FieldWrite,
	},
	/// A computed field derived at its place.
	Derive(u16),
}

/// Emits `ProcessTableFields` for a table that declares fields: every field in the order its schema
/// states, each place's value produced by the field's clauses (a computed field another field's
/// clause reads derived at its place in the order), then the deferred `ASSERT`s, then the
/// `COMPUTED` values stripped. A table whose producing clauses read each other in a cycle refuses
/// every record that is not an import.
///
/// Every clause compiles to its host pipeline before anything is emitted, so a clause the lowering
/// has no form for refuses the statement with the body as it was. A field's `PERMISSIONS` clauses
/// are `FULL`, `NONE` or a predicate over the post-image, which is judged with its reads not
/// permission-checked.
fn process_fields(em: &mut Emitter, cx: &mut Stages<'_, '_>) -> Result<(), IrError> {
	let run = cx.run;
	let declared = &run.stored.schema;
	let plan = declared_plan(run)?;
	if let Some(cycle) = &declared.cycle {
		// An import applies no clause, so it has no order to lack.
		let end = em.label()?;
		em.emit(Instr::Write(WriteInstr::JumpIfImport {
			target: end,
		}));
		em.emit(schema(SchemaInstr::Cycle {
			table: plan.table.clone(),
			fields: Arc::from(&**cycle),
		}));
		return em.bind(end);
	}
	if declared.unlowerable {
		return Err(IrError::Invariant {
			what: "ir_lower::stage: a field clause with no logical form, or one that holds a write no operator runs",
		});
	}
	let regions = &run.fields;
	let (mut defaults, mut values, mut asserts) =
		(regions.default.iter(), regions.value.iter(), regions.assert.iter());
	let (mut permissions, mut predicate_derives) =
		(regions.permission.iter(), regions.predicate_derive.iter());
	let mut derives = regions.derive.iter();
	let mut pipelines: Vec<Option<u16>> = vec![None; declared.fields.len()];
	let mut compiled = Vec::with_capacity(declared.steps.len());
	for step in declared.steps.iter() {
		match step {
			FieldStep::Derive(field) => {
				let region = next_region(&mut derives)?;
				let computed = &declared.computed;
				let pipeline =
					cx.host(em, |lowerer| derive_step_pipeline(lowerer, field, region, computed))?;
				compiled.push(Compiled::Derive(pipeline));
			}
			FieldStep::Field(field) => {
				let default = match field.default {
					Some(clause) => {
						let region = next_region(&mut defaults)?;
						Some(cx.host(em, |lowerer| field_clause_pipeline(lowerer, region, clause))?)
					}
					None => None,
				};
				let value = match field.value {
					Some(clause) => {
						let region = next_region(&mut values)?;
						Some(cx.host(em, |lowerer| field_clause_pipeline(lowerer, region, clause))?)
					}
					None => None,
				};
				if let Some(clause) = field.assert {
					let region = next_region(&mut asserts)?;
					let slot =
						pipelines.get_mut(usize::from(field.index)).ok_or(IrError::Invariant {
							what: "ir_lower::stage: a field step names a field the schema does not list",
						})?;
					*slot = Some(
						cx.host(em, |lowerer| field_clause_pipeline(lowerer, region, clause))?,
					);
				}
				let mut gate = |gate: &FieldGate| -> Result<FieldPermission, IrError> {
					Ok(match gate {
						FieldGate::Allow => FieldPermission::Allow,
						FieldGate::Deny => FieldPermission::Deny,
						FieldGate::Predicate {
							caller_bound,
							derives,
						} => {
							let region = next_region(&mut permissions)?;
							let fields = take_derivations(derives, &mut predicate_derives)?;
							FieldPermission::Predicate(cx.host(em, |lowerer| {
								field_predicate_pipeline(lowerer, region, *caller_bound, &fields)
							})?)
						}
					})
				};
				let write = FieldWrite {
					create: gate(&field.create)?,
					update: gate(&field.update)?,
				};
				compiled.push(Compiled::Field {
					index: field.index,
					default,
					value,
					asserted: field.assert.is_some(),
					write,
				});
			}
		}
	}
	let computed: Arc<[Idiom]> =
		declared.computed.iter().map(|name| Idiom::field(name.clone())).collect();
	let end = em.label()?;
	em.emit(schema(SchemaInstr::Begin {
		table: plan,
		end,
	}));
	for step in compiled {
		let (index, default, value, asserted, write) = match step {
			Compiled::Derive(pipeline) => {
				em.emit(schema(SchemaInstr::Derive {
					pipeline,
					computed: Arc::clone(&computed),
				}));
				continue;
			}
			Compiled::Field {
				index,
				default,
				value,
				asserted,
				write,
			} => (index, default, value, asserted, write),
		};
		em.emit(schema(SchemaInstr::Field {
			field: index,
			write,
		}));
		let head = em.label()?;
		em.bind(head)?;
		let done = em.label()?;
		em.emit(schema(SchemaInstr::NextPlace {
			done,
		}));
		if let Some(pipeline) = default {
			em.emit(schema(SchemaInstr::Default {
				pipeline,
			}));
		}
		match value {
			// A place holding nothing is produced by the `VALUE` and coerced after it; one holding
			// a value is coerced, rewritten, and coerced again. Only the last coercion holds the
			// value to the declared SQL type, so the type judges the value the field stores, once.
			Some(pipeline) => {
				let empty = em.label()?;
				em.emit(schema(SchemaInstr::JumpIfPlaceNone {
					target: empty,
				}));
				em.emit(schema(SchemaInstr::Coerce {
					sql_type: false,
				}));
				em.bind(empty)?;
				em.emit(schema(SchemaInstr::Value {
					pipeline,
				}));
				em.emit(schema(SchemaInstr::Coerce {
					sql_type: true,
				}));
			}
			None => {
				em.emit(schema(SchemaInstr::Coerce {
					sql_type: true,
				}));
			}
		}
		if asserted {
			em.emit(schema(SchemaInstr::DeferAssert));
		}
		if !write.admits_all() {
			em.emit(schema(SchemaInstr::Permit));
		}
		em.emit(schema(SchemaInstr::Land));
		em.emit(Instr::Jump {
			target: head,
		});
		em.bind(done)?;
		em.emit(schema(SchemaInstr::EndField));
	}
	if pipelines.iter().any(Option::is_some) {
		em.emit(schema(SchemaInstr::Asserts {
			pipelines: Arc::from(pipelines),
		}));
	}
	em.emit(schema(SchemaInstr::Finish {
		computed,
	}));
	em.bind(end)
}

/// The host pipeline that derives the field `field`, a `COMPUTED` field another field's clause
/// reads, onto the post-image it is handed and answers the image. A self-dereference reads the
/// image's stored fields when it carries a key under a name of `computed`, every `COMPUTED` field
/// of the table.
fn derive_step_pipeline(
	lowerer: &mut Lowerer<'_>,
	field: &DerivedField,
	region: RegionId,
	computed: &[Strand],
) -> Result<Pipeline, IrError> {
	observed_pipeline(lowerer, &[(field, region)], Some(computed))
}

/// Refuses the clause region `region` of a definition where it runs under an `AUTH LIMIT` that
/// narrows the caller and makes a closure: the body of a closure is lowered for the principal
/// that invokes it, and no unit of the narrowed principal is requested for it. A statement or a
/// call of a user-defined function under the limit is a unit planned for the narrowed principal.
/// `caller_bound` states whether the clause holds any of the three.
pub(super) fn refuse_narrowed(
	ir: &Ir,
	region: RegionId,
	caller_bound: bool,
) -> Result<(), IrError> {
	if !caller_bound {
		return Ok(());
	}
	let container = region_frame(ir, region)?;
	let held = ir
		.frame(container)
		.ok_or(IrError::Stale("ir_lower::stage: the frame of a definition's clause"))?;
	if held.eval.auth.is_none() {
		return Ok(());
	}
	let mut closure = false;
	ir.walk(region, WalkOrder::PreOrder, &mut |op| match ir.op(op).map(|data| &data.kind) {
		Some(OpKind::Expr(ExprOp::ClosureCreate(_))) => {
			closure = true;
			Walk::Stop
		}
		_ => Walk::Continue,
	});
	match closure {
		true => Err(not_yet(
			"ir_lower::stage: a clause of a definition under an AUTH LIMIT that makes a closure",
		)),
		false => Ok(()),
	}
}

/// Binds the arguments of the clause region `region` of a field's place: the row is the post-image
/// the pipeline runs over, and `$value` (which `$after` names too), `$before` and `$input` are the
/// parameters the host binds for the place, read where the clause reads them.
fn bind_place(lowerer: &mut Lowerer<'_>, region: RegionId) -> Result<(), IrError> {
	let ir = lowerer.ir;
	let args = lowerer.region_args(region)?;
	let [_, row, value, before, input] = <[_; 5]>::try_from(args).map_err(|_| {
		IrError::Stale(
			"ir_lower::stage: a field clause takes the timestamp, the row, the place's value, its old value and what the statement supplied",
		)
	})?;
	lowerer.bind_row(row, false);
	for (arg, name) in [(value, "value"), (before, "before"), (input, "input")] {
		if ir.uses(arg).is_empty() {
			continue;
		}
		let param = lowerer.em.param(ParamSpec {
			name: Strand::new(name),
			guard: None,
		})?;
		let dst = lowerer.em.reg()?;
		lowerer.emit(Instr::LoadParam {
			param,
			dst,
			frame: None,
		});
		lowerer.bind_arg(arg, Operand::Reg(dst));
	}
	Ok(())
}

/// The host pipeline over the post-image that emits the value of the `DEFAULT`, `VALUE` or `ASSERT`
/// clause region `region` of a field, in the privileged frame the host binds for the field's
/// place.
fn field_clause_pipeline(
	lowerer: &mut Lowerer<'_>,
	region: RegionId,
	clause: FieldClause,
) -> Result<Pipeline, IrError> {
	let ir = lowerer.ir;
	let container = region_frame(ir, region)?;
	refuse_narrowed(ir, region, clause.caller_bound)?;
	bind_place(lowerer, region)?;
	lowerer.in_host_frame(container, |lowerer| {
		let value = lowerer.region_value(region)?;
		let out = lowerer.em.reg()?;
		lowerer.emit(Instr::Load {
			dst: out,
			src: value,
		});
		emit_clause(lowerer, out)
	})
}

/// The host pipeline over the post-image that emits the verdict of the field permission predicate
/// region `region`, in the frame of a permission a write triggers (reads not permission-checked),
/// with the computed fields `fields` derived onto the image first, each landing before the next is
/// evaluated.
fn field_predicate_pipeline(
	lowerer: &mut Lowerer<'_>,
	region: RegionId,
	caller_bound: bool,
	fields: &[(&DerivedField, RegionId)],
) -> Result<Pipeline, IrError> {
	let ir = lowerer.ir;
	let container = region_frame(ir, region)?;
	let held = ir
		.frame(container)
		.ok_or(IrError::Stale("ir_lower::stage: the frame of a field permission"))?;
	if held.eval.mode != EvalMode::MutablePermission {
		return Err(IrError::Invariant {
			what: "ir_lower::stage: a field permission predicate outside the frame of a permission a write triggers",
		});
	}
	refuse_narrowed(ir, region, caller_bound)?;
	bind_place(lowerer, region)?;
	if !fields.is_empty() {
		derive_fields(lowerer, fields, Derive::Gate, None)?;
	}
	lowerer.in_host_frame(container, |lowerer| {
		let verdict = lowerer.region_value(region)?;
		let out = lowerer.em.reg()?;
		lowerer.emit(Instr::Load {
			dst: out,
			src: verdict,
		});
		emit_clause(lowerer, out)
	})
}

/// The host pipeline over the snapshot that emits what the condition region `region` yields.
fn condition_pipeline(lowerer: &mut Lowerer<'_>, region: RegionId) -> Result<Pipeline, IrError> {
	bind_snapshot(lowerer, region)?;
	let verdict = lowerer.region_value(region)?;
	let out = lowerer.em.reg()?;
	lowerer.emit(Instr::Load {
		dst: out,
		src: verdict,
	});
	emit_clause(lowerer, out)
}

/// The host pipeline over the snapshot that evaluates the data clause region `region`.
///
/// With `many` it emits one array holding the value of each assignment of the region, in written
/// order, all over the one snapshot; otherwise it emits the one value the region's content op
/// stores. A region that ends in a raise, a value of the clause that throws, raises there and
/// stores nothing past it. Each value the clause stores is a context of its own: a `RETURN` a
/// statement in it signals is that value ([`Lowerer::context_value`]). The subscripts and filters
/// of a place are not evaluated here: the places a clause writes at are the write's own steps.
fn data_pipeline(
	lowerer: &mut Lowerer<'_>,
	region: RegionId,
	many: bool,
) -> Result<Pipeline, IrError> {
	bind_snapshot(lowerer, region)?;
	let ir = lowerer.ir;
	let out = lowerer.em.reg()?;
	if many {
		lowerer.emit(Instr::NewArray {
			dst: out,
		});
	}
	let mut stored = false;
	let mut raised = false;
	let context = ends_context(ir, region);
	for op in ops_in(ir, region) {
		let data = ir.op(op).ok_or(IrError::Stale("ir_lower::stage: an op of a clause is gone"))?;
		let value = match &data.kind {
			OpKind::Expr(ExprOp::Throw | ExprOp::Raise(_)) => {
				lowerer.raise_terminator(op)?;
				raised = true;
				break;
			}
			OpKind::Logical(LogOp::Assign(_))
			| OpKind::Phys(PhysOp::Assign(_))
			| OpKind::Logical(LogOp::Content {
				..
			})
			| OpKind::Phys(PhysOp::Content {
				..
			}) => *data
				.operands
				.first()
				.ok_or(IrError::Stale("ir_lower::stage: a clause op holds no value"))?,
			OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_) => continue,
		};
		let operand = match context {
			true => lowerer.context_value(|lowerer| lowerer.value(value))?,
			false => lowerer.value(value)?,
		};
		if many {
			lowerer.emit(Instr::ArrayPush {
				array: out,
				value: operand,
			});
		} else if std::mem::replace(&mut stored, true) {
			return Err(IrError::Stale("ir_lower::stage: a clause stores two values"));
		} else {
			lowerer.emit(Instr::Load {
				dst: out,
				src: operand,
			});
		}
	}
	if !many && !stored && !raised {
		return Err(IrError::Stale("ir_lower::stage: a clause stores no value"));
	}
	emit_clause(lowerer, out)
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::super::tests::Builder;
	use super::*;
	use crate::doc::Action;
	use crate::exec::field_path::FieldPathPart;
	use crate::exec::ir::core::ir::Ir;
	use crate::exec::ir::iface::ids::{OpId, ValueId};
	use crate::exec::ir::iface::region_kind::StatementCommit;
	use crate::exec::ir::iface::statements::{Place, PlacePart};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::logical::ops::{
		AssignAttrs, FieldClauses, FieldShape, FieldsShape, IndexDefs, RepairField, RepairTable,
		TableDefs, WriteIndexes, WriteRepairs, WriteSchema, WriteViews,
	};
	use crate::exec::ir::logical::stages::view_table_for_test;
	use crate::exec::ir::logical::verify::timestamp_arg;
	use crate::exec::ir::phys::ops::ProjectedFieldShape;
	use crate::exec::operators::write::stage::{LightweightReason, TableTypeFor};
	use crate::exec::vm::program::UnitId;
	use crate::expr::logical::{EvalFrame, OutputPath};
	use crate::expr::{AssignOperator, BinaryOperator};
	use crate::iam::Action as GateAction;
	use crate::val::Value;

	/// An update's run over a table whose images are plain, for a caller whose reads are
	/// permission-checked. It stores its record through `StoreRecordData`.
	fn run() -> StageRun {
		StageRun {
			stages: vec![WriteStageKind::StoreRecordData, WriteStageKind::StoreIndexData],
			cond: None,
			data: None,
			output: OutputShape::After,
			returns: None,
			form: WriteForm::Update,
			gates: WriteGates::default(),
			hosted: Vec::new(),
			derived: WriteDerived::default(),
			image: WriteImage {
				view: ImageView::Plain,
				keyed: false,
				id_default: false,
			},
			stored: WriteStored::default(),
			fields: FieldRegions::default(),
			id_default: None,
			indexes: Vec::new(),
			views: Vec::new(),
			repairs: Vec::new(),
			layers: Vec::new(),
			checked: true,
			nesting: StatementNesting::default(),
		}
	}

	/// The facts of a `SCHEMAFULL` table `person` that stores `kind` of everything, in a statement
	/// that cannot change the catalog.
	fn storing(kind: Stored) -> WriteStored {
		WriteStored {
			table: Some(surrealdb_strand::TableName::new(surrealdb_strand::Strand::new("person"))),
			schemafull: true,
			catalog_fixed: true,
			fields: kind,
			references: kind,
			indexes: kind,
			events: kind,
			views: kind,
			referenced: kind,
			edges: kind,
			schema: WriteSchema::default(),
			reference_fields: Box::default(),
			materialized: Box::default(),
			index_set: WriteIndexes::default(),
			view_set: WriteViews::default(),
			repair_set: WriteRepairs::default(),
			image_layers: ImageLayers::default(),
		}
	}

	fn stored_run(stored: WriteStored) -> StageRun {
		StageRun {
			stored,
			..run()
		}
	}

	fn viewed(view: ImageView) -> StageRun {
		StageRun {
			image: WriteImage {
				view,
				keyed: false,
				id_default: false,
			},
			..run()
		}
	}

	fn gated(form: WriteForm, clause: GateClause) -> StageRun {
		StageRun {
			form,
			gates: WriteGates {
				create: clause,
				update: clause,
				delete: clause,
				select: clause,
			},
			..run()
		}
	}

	/// The code `stage` emits under `run` over the container `ir`, with every jump label rewritten
	/// to the index it names, and the host pipelines the stage built.
	fn emitted_in(
		ir: &Ir,
		run: &StageRun,
		stage: WriteStageKind,
	) -> Result<(Vec<Instr>, Vec<Pipeline>), IrError> {
		emitted_program(ir, run, stage).map(|(code, program)| (code, program.host_code))
	}

	/// [`emitted_in`] under a lowering context, which the host pipelines of a view need.
	fn emitted_in_context(
		ir: &Ir,
		run: &StageRun,
		stage: WriteStageKind,
	) -> Result<(Vec<Instr>, Vec<Pipeline>), IrError> {
		let mut em = Emitter::new();
		let skip = em.label()?;
		let mut units = LowerCx::detached();
		{
			let mut cx = Stages::new(
				run,
				ir,
				LowerLimits::default(),
				crate::exec::ir::analysis::cx::builtin_registry(),
			)
			.in_context(&mut units);
			emit_stage(&mut em, &mut cx, stage, skip)?;
		}
		em.bind(skip)?;
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once)?;
		let code = pipeline.code.clone();
		let program = em.finish_program(vec![pipeline], crate::exec::AccessMode::ReadOnly)?;
		Ok((code, program.host_code))
	}

	/// [`emitted_in`], with the whole program the stage's code and host pipelines are held by.
	fn emitted_program(
		ir: &Ir,
		run: &StageRun,
		stage: WriteStageKind,
	) -> Result<(Vec<Instr>, crate::exec::vm::program::Program), IrError> {
		let mut em = Emitter::new();
		let skip = em.label()?;
		let mut cx = Stages::new(
			run,
			ir,
			LowerLimits::default(),
			crate::exec::ir::analysis::cx::builtin_registry(),
		);
		emit_stage(&mut em, &mut cx, stage, skip)?;
		em.bind(skip)?;
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once)?;
		let code = pipeline.code.clone();
		let program = em.finish_program(vec![pipeline], crate::exec::AccessMode::ReadOnly)?;
		Ok((code, program))
	}

	/// The code `stage` emits under `run`, which reads no clause.
	fn emitted(run: &StageRun, stage: WriteStageKind) -> Result<Vec<Instr>, IrError> {
		emitted_in(&Builder::new().ir, run, stage).map(|(code, _)| code)
	}

	/// The code `stage` emits under `run` with the unit table of a lowering context, and the units
	/// the code asked that context for.
	fn emitted_with_units(
		run: &StageRun,
		stage: WriteStageKind,
	) -> Result<(Vec<Instr>, Vec<super::super::units::Needed>), IrError> {
		let ir = Builder::new().ir;
		let mut em = Emitter::new();
		let skip = em.label()?;
		let mut units = LowerCx::detached();
		{
			let mut cx = Stages::new(
				run,
				&ir,
				LowerLimits::default(),
				crate::exec::ir::analysis::cx::builtin_registry(),
			)
			.in_context(&mut units);
			emit_stage(&mut em, &mut cx, stage, skip)?;
		}
		em.bind(skip)?;
		em.emit(Instr::EndRow);
		let code = em.finish_pipeline(Source::Once)?.code;
		Ok((code, units.take_needed()))
	}

	fn code(stage: WriteStageKind) -> Vec<Instr> {
		match emitted(&run(), stage) {
			Ok(code) => code,
			Err(error) => panic!("{stage:?} lowers: {error}"),
		}
	}

	#[test]
	fn a_record_the_statement_named_that_storage_lacks_is_skipped() {
		assert_eq!(
			code(WriteStageKind::CheckRecordExists),
			[
				storage(StorageInstr::JumpIf {
					fact: StorageFact::Absent,
					when: true,
					target: 1,
				}),
				Instr::EndRow,
			]
		);
	}

	#[test]
	fn the_structural_checks_emit_one_refusal_each() {
		assert_eq!(
			code(WriteStageKind::CheckTableNotView),
			[storage(StorageInstr::RefuseView), Instr::EndRow]
		);
		assert_eq!(
			code(WriteStageKind::CheckTableNotLightweight(LightweightReason::Updated)),
			[
				storage(StorageInstr::RefuseLightweight {
					reason: LightweightReason::Updated,
				}),
				Instr::EndRow
			]
		);
		assert_eq!(
			code(WriteStageKind::CheckTableType(TableTypeFor::Create)),
			[
				storage(StorageInstr::RefuseTableType {
					subject: TableTypeFor::Create,
				}),
				Instr::EndRow
			]
		);
	}

	#[test]
	fn a_stored_record_is_written_create_only_in_the_iteration_it_began() {
		let jump = |fact, when, target| {
			storage(StorageInstr::JumpIf {
				fact,
				when,
				target,
			})
		};
		assert_eq!(
			code(WriteStageKind::StoreRecordData),
			[
				Instr::Write(WriteInstr::ReachedStore),
				jump(StorageFact::Unmodified, true, 8),
				jump(StorageFact::TableDropped, true, 8),
				jump(StorageFact::TableLightweight, true, 8),
				jump(StorageFact::IterationInitial, false, 7),
				storage(StorageInstr::Store {
					create: true,
					cache: true,
				}),
				Instr::Jump {
					target: 8,
				},
				storage(StorageInstr::Store {
					create: false,
					cache: false,
				}),
				Instr::EndRow,
			]
		);
	}

	/// The cache flag of the one create-only store `run`'s `StoreRecordData` lowers to.
	fn created_cache(run: &StageRun) -> bool {
		let code = match emitted(run, WriteStageKind::StoreRecordData) {
			Ok(code) => code,
			Err(error) => panic!("StoreRecordData lowers: {error}"),
		};
		let mut creates = code.iter().filter_map(|instr| {
			if let Instr::Write(WriteInstr::Storage(StorageInstr::Store {
				create: true,
				cache,
			})) = instr
			{
				Some(*cache)
			} else {
				None
			}
		});
		let cache = creates.next().expect("a create-only store");
		assert_eq!(creates.next(), None, "one create-only store");
		cache
	}

	#[test]
	fn a_create_returning_nothing_on_a_table_without_events_keeps_no_record_cached() {
		let quiet = |output, events| StageRun {
			output,
			..stored_run(storing(events))
		};
		assert!(!created_cache(&quiet(OutputShape::None, Stored::Nothing)));
		assert!(created_cache(&quiet(OutputShape::None, Stored::Declared)));
		assert!(created_cache(&quiet(OutputShape::Null, Stored::Nothing)));
		assert!(created_cache(&quiet(OutputShape::After, Stored::Nothing)));
		assert!(created_cache(&quiet(OutputShape::Default, Stored::Nothing)));
		let unfixed = StageRun {
			output: OutputShape::None,
			stored: WriteStored {
				catalog_fixed: false,
				..storing(Stored::Nothing)
			},
			..run()
		};
		assert!(created_cache(&unfixed));
	}

	#[test]
	fn a_released_doc_id_is_marked_pending_while_a_build_replays() {
		let jump = |fact, when, target| {
			storage(StorageInstr::JumpIf {
				fact,
				when,
				target,
			})
		};
		// No fact about the table's `DROP` clause gates the release: the purge removes the stored
		// record of a `DROP` table as it does any other.
		assert_eq!(
			code(WriteStageKind::ReclaimDocId),
			[
				jump(StorageFact::DocIdDeferred, false, 3),
				storage(StorageInstr::MarkDocIdPending),
				Instr::Jump {
					target: 6
				},
				jump(StorageFact::DocIdConsumer, true, 5),
				jump(StorageFact::GraphDocIdsSettled, false, 6),
				storage(StorageInstr::ReleaseDocId),
				Instr::EndRow,
			]
		);
	}

	#[test]
	fn the_observers_emit_one_instruction() {
		assert_eq!(
			code(WriteStageKind::ProcessChangefeeds),
			[Instr::Write(WriteInstr::Observe(ObserveInstr::RecordChange)), Instr::EndRow]
		);
		assert_eq!(
			code(WriteStageKind::ProcessTableLives(Action::Create)),
			[Instr::Write(WriteInstr::Observe(ObserveInstr::Lives(Action::Create))), Instr::EndRow]
		);
	}

	#[test]
	fn a_statement_with_no_clause_has_the_data_stages_of_no_clause() {
		assert_eq!(
			code(WriteStageKind::CheckWhereCondition),
			[data(DataInstr::Matched), Instr::EndRow]
		);
		assert_eq!(code(WriteStageKind::ComputeInputData), [Instr::EndRow]);
		assert_eq!(
			code(WriteStageKind::ProcessRecordData),
			[data(DataInstr::ApplyInput), Instr::EndRow]
		);
	}

	#[test]
	fn a_return_that_shows_no_image_sets_the_output_unless_the_import_wrote_nothing() {
		for (output, set) in
			[(OutputShape::None, WriteOutput::None), (OutputShape::Null, WriteOutput::Null)]
		{
			let run = StageRun {
				output,
				..run()
			};
			assert_eq!(
				emitted(&run, WriteStageKind::OutputWrite).expect("a hidden output lowers"),
				[
					Instr::Write(WriteInstr::JumpIfImport {
						target: 3
					}),
					Instr::Write(WriteInstr::ClearCurrentReduced),
					Instr::Write(WriteInstr::SetOutput {
						output: set
					}),
					Instr::EndRow,
				]
			);
		}
	}

	/// A stage the lowering has no form for names itself in its refusal. `GenerateRecordId` has a
	/// form for an `id` DEFAULT: the stage hosts it as a region
	/// (`the_default_of_the_id_field_is_a_host_pipeline_of_the_stage_that_generates_the_id`), and
	/// an image that states one the stage hosts no region for is an invariant break.
	#[test]
	fn a_clause_the_lowering_has_no_form_for_names_its_stage() {
		for (run, stage) in [
			(viewed(ImageView::Dynamic), WriteStageKind::OutputWrite),
			(run(), WriteStageKind::StoreIndexData),
		] {
			assert!(
				matches!(emitted(&run, stage), Err(IrError::NotYet { what }) if what.starts_with("ir_lower::stage: ")),
				"{stage:?} answers NotYet"
			);
		}
		let unhosted = StageRun {
			image: WriteImage {
				view: ImageView::Plain,
				keyed: false,
				id_default: true,
			},
			..run()
		};
		assert!(matches!(
			emitted(&unhosted, WriteStageKind::GenerateRecordId),
			Err(IrError::Invariant { .. })
		));
	}

	/// The code of a gate stage under `run`.
	fn gate_code(run: &StageRun, stage: WriteStageKind) -> Vec<Instr> {
		match emitted(run, stage) {
			Ok(code) => code,
			Err(error) => panic!("{stage:?} lowers: {error}"),
		}
	}

	#[test]
	fn a_full_clause_admits_every_record_and_a_refusing_one_skips_it() {
		let skipped = |action: GateAction| {
			vec![
				gate(GateInstr::JumpIfNoId {
					target: 3,
				}),
				gate(GateInstr::JumpIfUnchecked {
					action,
					target: 3,
				}),
				Instr::Jump {
					target: 3,
				},
				Instr::EndRow,
			]
		};
		for (stage, action) in [
			(WriteStageKind::CheckCreatePermissions, GateAction::Edit),
			(WriteStageKind::CheckUpdatePermissions, GateAction::Edit),
			(WriteStageKind::CheckDeletePermissions, GateAction::Edit),
			(WriteStageKind::CheckSelectPermissions(RecordImage::Current), GateAction::View),
		] {
			let full = gated(WriteForm::Update, GateClause::Full);
			assert_eq!(gate_code(&full, stage), [Instr::EndRow], "{stage:?}");
			for refusing in [GateClause::None, GateClause::Absent] {
				assert_eq!(
					gate_code(&gated(WriteForm::Update, refusing), stage),
					skipped(action),
					"{stage:?} {refusing:?}"
				);
			}
		}
	}

	#[test]
	fn the_create_check_before_the_data_clause_refuses_only_a_none_clause() {
		let stage = WriteStageKind::CheckPermissionsQuickCreate;
		let predicate = GateClause::Predicate {
			derives: false,
		};
		assert_eq!(gate_code(&gated(WriteForm::Create, GateClause::Full), stage), [Instr::EndRow]);
		assert_eq!(gate_code(&gated(WriteForm::Create, predicate), stage), [Instr::EndRow]);
		assert_eq!(gate_code(&gated(WriteForm::Create, GateClause::None), stage).len(), 4);
	}

	#[test]
	fn the_recheck_of_an_update_judges_only_a_predicate() {
		let stage = WriteStageKind::RecheckUpdatePermissions;
		for clause in [GateClause::Full, GateClause::None, GateClause::Absent] {
			assert_eq!(gate_code(&gated(WriteForm::Update, clause), stage), [Instr::EndRow]);
		}
		// A predicate the gate stage does not host is a broken invariant, never an admission.
		assert!(matches!(
			emitted(
				&gated(
					WriteForm::Update,
					GateClause::Predicate {
						derives: false
					}
				),
				stage
			),
			Err(IrError::Invariant { .. })
		));
	}

	/// A stage op of `stage` hosting a `PermissionFrame` region built under `frame`, whose
	/// predicate compares the field `owner` of the image with 1, and then one
	/// `FieldFrame(Computed)` region per entry of `derivations`, built under `derived`, whose body
	/// is the number of the entry. Answers what the op hosts.
	fn hosting(
		b: &mut Builder,
		stage: WriteStageKind,
		frame: EvalFrame,
		derived: EvalFrame,
		derivations: usize,
	) -> Hosted {
		hosting_with(b, stage, frame, derived, derivations, |b, at| {
			b.literal(Value::from(at as i64))
		})
	}

	/// [`hosting`], with the body of each derivation built by `body` from its number.
	fn hosting_with(
		b: &mut Builder,
		stage: WriteStageKind,
		frame: EvalFrame,
		derived: EvalFrame,
		derivations: usize,
		body: impl Fn(&mut Builder, usize) -> ValueId,
	) -> Hosted {
		let (id, derived) = (b.frame(frame), b.frame(derived));
		let owner = b.add_logical(
			LogOp::WriteStage {
				stage,
				mark: crate::exec::ir::logical::ops::WriteMark::Apply,
			},
			&[],
		);
		let arguments = [TypeKnowledge::Unknown, TypeKnowledge::Unknown];
		b.in_frame(id, |b| {
			b.region_with_args(owner, RegionKind::PermissionFrame, &arguments, |b, args| {
				let held = b.add(
					ExprOp::Access {
						field: Strand::new("owner"),
					},
					&[args[1]],
				);
				let one = b.literal(Value::from(1_i64));
				b.binary(BinaryOperator::Equal, held, one)
			});
		});
		b.in_frame(derived, |b| {
			for at in 0..derivations {
				let kind = RegionKind::FieldFrame(FieldRole::Computed);
				b.region_with_args(owner, kind, &arguments, |b, _| body(b, at));
			}
		});
		let regions = b.ir.op(owner).expect("the stage op").regions.to_vec();
		Hosted {
			predicate: Some(regions[0]),
			derivations: regions[1..].to_vec(),
		}
	}

	/// A gate stage op of `stage` hosting a predicate under `frame` and no derivation.
	fn predicate_region(b: &mut Builder, stage: WriteStageKind, frame: EvalFrame) -> Hosted {
		hosting(b, stage, frame, EvalFrame::caller(), 0)
	}

	/// A run that states `clause` for every gate and holds `hosted` for `stage`.
	fn predicated(clause: GateClause, stage: WriteStageKind, hosted: Hosted) -> StageRun {
		StageRun {
			hosted: vec![(stage, hosted)],
			..gated(WriteForm::Update, clause)
		}
	}

	const OWNED: GateClause = GateClause::Predicate {
		derives: false,
	};

	const DERIVING: GateClause = GateClause::Predicate {
		derives: true,
	};

	/// A computed field `name` of no kind whose body has no caller-bound code.
	fn derived_field(name: &str) -> DerivedField {
		DerivedField {
			name: Strand::new(name),
			kind: None,
			caller_bound: false,
		}
	}

	#[test]
	fn a_write_predicate_is_judged_over_a_copy_of_the_post_image_in_its_gate_frame() {
		let mut b = Builder::new();
		for stage in [
			WriteStageKind::CheckCreatePermissions,
			WriteStageKind::CheckUpdatePermissions,
			WriteStageKind::CheckDeletePermissions,
			WriteStageKind::RecheckUpdatePermissions,
		] {
			let hosted = predicate_region(&mut b, stage, EvalFrame::mutable_permission(None));
			let (code, program) = emitted_program(&b.ir, &predicated(OWNED, stage, hosted), stage)
				.expect("it lowers");
			assert_eq!(
				code,
				[
					gate(GateInstr::JumpIfNoId {
						target: 3
					}),
					gate(GateInstr::JumpIfUnchecked {
						action: GateAction::Edit,
						target: 3
					}),
					gate(GateInstr::Judge {
						image: RecordImage::Current,
						plain: 0,
						derived: None,
						skip: 3
					}),
					Instr::EndRow,
				],
				"{stage:?}"
			);
			assert_eq!(
				program.permissions,
				vec![PermissionSpec {
					mode: FrameMode::Gate(GuardKind::Write),
					auth: None,
					column: false,
				}],
				"{stage:?}"
			);
			let [verdict] = program.host_code.as_slice() else {
				panic!("the predicate is one host pipeline");
			};
			assert!(matches!(verdict.source, Source::Once));
			assert!(matches!(
				verdict.code[verdict.code.len() - 2..],
				[Instr::Emit { .. }, Instr::EndRow]
			));
		}
	}

	#[test]
	fn a_select_predicate_is_judged_in_a_read_gate_frame_over_the_image_it_names() {
		let mut b = Builder::new();
		let stage = WriteStageKind::CheckSelectPermissions(RecordImage::Initial);
		let hosted = predicate_region(&mut b, stage, EvalFrame::permission(None));
		let (code, program) =
			emitted_program(&b.ir, &predicated(OWNED, stage, hosted), stage).expect("it lowers");
		assert_eq!(
			code[..3],
			[
				gate(GateInstr::JumpIfNoId {
					target: 3
				}),
				gate(GateInstr::JumpIfUnchecked {
					action: GateAction::View,
					target: 3
				}),
				gate(GateInstr::Judge {
					image: RecordImage::Initial,
					plain: 0,
					derived: None,
					skip: 3
				}),
			]
		);
		assert_eq!(program.permissions[0].mode, FrameMode::Gate(GuardKind::Read));
	}

	#[test]
	fn a_predicate_outside_the_frame_of_its_guard_is_an_invariant_break() {
		let mut b = Builder::new();
		let stage = WriteStageKind::CheckCreatePermissions;
		let hosted = predicate_region(&mut b, stage, EvalFrame::permission(None));
		assert!(matches!(
			emitted_in(&b.ir, &predicated(OWNED, stage, hosted), stage),
			Err(IrError::Invariant { .. })
		));
	}

	#[test]
	fn a_predicate_that_reads_computed_fields_judges_a_copy_they_are_derived_onto() {
		let mut b = Builder::new();
		let stage = WriteStageKind::CheckUpdatePermissions;
		let hosted = hosting(
			&mut b,
			stage,
			EvalFrame::mutable_permission(None),
			EvalFrame::permission(None),
			2,
		);
		let fields = [derived_field("net"), derived_field("total")];
		let run = StageRun {
			derived: WriteDerived {
				update: fields.to_vec().into(),
				..WriteDerived::default()
			},
			..predicated(DERIVING, stage, hosted)
		};
		let (code, program) = emitted_program(&b.ir, &run, stage).expect("it lowers");
		assert_eq!(
			code[2],
			gate(GateInstr::Judge {
				image: RecordImage::Current,
				plain: 0,
				derived: Some(1),
				skip: 3
			})
		);
		// The verdict is judged in the gate frame, and each field in the derived frame, which
		// does not check the reads of its body.
		assert_eq!(
			program.permissions,
			vec![
				PermissionSpec {
					mode: FrameMode::Gate(GuardKind::Write),
					auth: None,
					column: false,
				},
				PermissionSpec {
					mode: FrameMode::Derived,
					auth: None,
					column: false,
				},
			]
		);
		let [plain, derived] = program.host_code.as_slice() else {
			panic!("the verdict and the derived verdict are two host pipelines");
		};
		let lands = |pipeline: &Pipeline| {
			pipeline.code.iter().filter(|instr| matches!(instr, Instr::LandRow { .. })).count()
		};
		assert_eq!((lands(plain), lands(derived)), (0, 2));
		assert!(matches!(derived.code[0], Instr::BeginComputing { .. }));
		let begun =
			derived.code.iter().position(|instr| matches!(instr, Instr::BeginComputing { .. }));
		let emitted = derived.code.iter().position(|instr| matches!(instr, Instr::Emit { .. }));
		assert!(begun < emitted, "the fields are derived before the verdict is emitted");
	}

	#[test]
	fn a_derivation_the_write_states_but_the_stage_does_not_host_is_never_skipped() {
		let mut b = Builder::new();
		let stage = WriteStageKind::CheckUpdatePermissions;
		let hosted = hosting(
			&mut b,
			stage,
			EvalFrame::mutable_permission(None),
			EvalFrame::permission(None),
			1,
		);
		let mut run = predicated(DERIVING, stage, hosted);
		run.derived.update = vec![derived_field("net"), derived_field("total")].into();
		assert!(matches!(emitted_in(&b.ir, &run, stage), Err(IrError::Invariant { .. })));
		// A clause that states a derivation its fields do not list contradicts itself.
		run.derived.update = Vec::new().into();
		assert!(matches!(emitted_in(&b.ir, &run, stage), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn a_computed_field_is_derived_only_in_the_frame_of_its_stage() {
		let mut b = Builder::new();
		let stage = WriteStageKind::CheckUpdatePermissions;
		// A gate derives in a permission frame, never the computed frame the observed fields use.
		let hosted = hosting(
			&mut b,
			stage,
			EvalFrame::mutable_permission(None),
			EvalFrame::computed(None),
			1,
		);
		let mut run = predicated(DERIVING, stage, hosted);
		run.derived.update = vec![derived_field("net")].into();
		assert!(matches!(emitted_in(&b.ir, &run, stage), Err(IrError::Invariant { .. })));
	}

	/// A derivation under an `AUTH LIMIT` that narrows the caller lowers when its body holds a
	/// statement or a call, which run as units planned for the narrowed principal, and answers
	/// `NotYet` when it makes a closure. With no limit the body is lowered for the caller.
	#[test]
	fn a_derivation_under_an_auth_limit_that_makes_a_closure_is_not_yet() {
		let stage = WriteStageKind::CheckUpdatePermissions;
		let limit = surrealdb_iam::AuthLimit::new(surrealdb_iam::Level::Root, None);
		for (auth, closure, lowers) in
			[(None, false, true), (Some(limit.clone()), false, true), (Some(limit), true, false)]
		{
			let mut b = Builder::new();
			let hosted = hosting_with(
				&mut b,
				stage,
				EvalFrame::mutable_permission(None),
				EvalFrame::permission(auth.clone()),
				1,
				|b, at| match closure {
					true => b.closure(),
					false => b.literal(Value::from(at as i64)),
				},
			);
			let mut run = predicated(DERIVING, stage, hosted);
			run.derived.update = vec![DerivedField {
				caller_bound: true,
				..derived_field("net")
			}]
			.into();
			let lowered = emitted_in(&b.ir, &run, stage);
			match lowers {
				true => assert!(lowered.is_ok(), "{auth:?} {closure}: {:?}", lowered.err()),
				false => assert!(matches!(lowered, Err(IrError::NotYet { .. }))),
			}
		}
	}

	/// A stage op of `MaterialiseObservedFields` hosting `derivations` computed regions under the
	/// computed frame, in a run of a table that stores fields and states `fields` as the ones it
	/// derives.
	fn observing(
		b: &mut Builder,
		frame: EvalFrame,
		derivations: usize,
		fields: Option<Vec<DerivedField>>,
	) -> StageRun {
		observing_with(b, frame, derivations, fields, |b, at| b.literal(Value::from(at as i64)))
	}

	/// [`observing`], with the body of each derivation built by `body` from its number.
	fn observing_with(
		b: &mut Builder,
		frame: EvalFrame,
		derivations: usize,
		fields: Option<Vec<DerivedField>>,
		body: impl Fn(&mut Builder, usize) -> ValueId,
	) -> StageRun {
		let id = b.frame(frame);
		let stage = WriteStageKind::MaterialiseObservedFields;
		let owner = b.add_logical(
			LogOp::WriteStage {
				stage,
				mark: crate::exec::ir::logical::ops::WriteMark::Apply,
			},
			&[],
		);
		b.in_frame(id, |b| {
			for at in 0..derivations {
				b.region_with_args(
					owner,
					RegionKind::FieldFrame(FieldRole::Computed),
					&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
					|b, _| body(b, at),
				);
			}
		});
		let regions = b.ir.op(owner).expect("the stage op").regions.to_vec();
		let hosted = Hosted {
			predicate: None,
			derivations: regions,
		};
		StageRun {
			hosted: vec![(stage, hosted)],
			derived: WriteDerived {
				observed: fields.map(Into::into),
				..WriteDerived::default()
			},
			..stored_run(storing(Stored::Declared))
		}
	}

	#[test]
	fn the_computed_fields_of_a_table_are_derived_onto_the_images_of_an_observed_record() {
		let mut b = Builder::new();
		let fields = vec![derived_field("net"), derived_field("total")];
		let run = observing(&mut b, EvalFrame::computed(None), 2, Some(fields));
		let stage = WriteStageKind::MaterialiseObservedFields;
		let (code, program) = emitted_program(&b.ir, &run, stage).expect("it lowers");
		assert_eq!(
			code,
			[
				schema(SchemaInstr::Observe {
					pipelines: Arc::from(vec![0])
				}),
				Instr::EndRow
			]
		);
		// Each body runs in the computed frame, which checks the reads it makes.
		assert_eq!(
			program.permissions,
			vec![PermissionSpec {
				mode: FrameMode::Computed,
				auth: None,
				column: false,
			}]
		);
		let [derived] = program.host_code.as_slice() else {
			panic!("the observed fields are one host pipeline");
		};
		assert_eq!(
			derived.code.iter().filter(|instr| matches!(instr, Instr::LandRow { .. })).count(),
			2
		);
		let tail = &derived.code[derived.code.len() - 2..];
		assert!(matches!(
			tail,
			[
				Instr::Emit {
					src: ROW
				},
				Instr::EndRow
			]
		));
	}

	#[test]
	fn a_table_with_no_computed_field_derives_nothing_but_one_that_is_not_stated_is_refused() {
		let mut b = Builder::new();
		let stage = WriteStageKind::MaterialiseObservedFields;
		let none = observing(&mut b, EvalFrame::computed(None), 0, Some(Vec::new()));
		assert_eq!(emitted(&none, stage).expect("it lowers"), [Instr::EndRow]);
		let unstated = observing(&mut b, EvalFrame::computed(None), 0, None);
		assert!(matches!(emitted_in(&b.ir, &unstated, stage), Err(IrError::Invariant { .. })));
		// A field no region derives, and a region of the wrong frame, are never skipped or
		// derived in the wrong mode.
		let missing =
			observing(&mut b, EvalFrame::computed(None), 0, Some(vec![derived_field("net")]));
		assert!(matches!(emitted_in(&b.ir, &missing, stage), Err(IrError::Invariant { .. })));
		let wrong =
			observing(&mut b, EvalFrame::permission(None), 1, Some(vec![derived_field("net")]));
		assert!(matches!(emitted_in(&b.ir, &wrong, stage), Err(IrError::Invariant { .. })));
	}

	/// An observed field whose body holds a statement or a call lowers with or without an `AUTH
	/// LIMIT`; one whose body makes a closure answers `NotYet` only under a limit that narrows the
	/// caller.
	#[test]
	fn an_observed_field_making_a_closure_is_not_yet_only_under_an_auth_limit() {
		let stage = WriteStageKind::MaterialiseObservedFields;
		let bound = || {
			Some(vec![DerivedField {
				caller_bound: true,
				..derived_field("net")
			}])
		};
		let limit = surrealdb_iam::AuthLimit::new(surrealdb_iam::Level::Root, None);
		for (auth, closure, lowers) in
			[(None, false, true), (Some(limit.clone()), false, true), (Some(limit), true, false)]
		{
			let mut b = Builder::new();
			let run =
				observing_with(&mut b, EvalFrame::computed(auth.clone()), 1, bound(), |b, at| {
					match closure {
						true => b.closure(),
						false => b.literal(Value::from(at as i64)),
					}
				});
			let lowered = emitted_in(&b.ir, &run, stage);
			match lowers {
				true => assert!(lowered.is_ok(), "{auth:?} {closure}: {:?}", lowered.err()),
				false => assert!(matches!(lowered, Err(IrError::NotYet { .. }))),
			}
		}
	}

	#[test]
	fn tenant_confinement_runs_before_the_target_refusal() {
		let tenant = gate(GateInstr::Tenant);
		let refused =
			|form| gate_code(&gated(form, GateClause::None), WriteStageKind::CheckRecordUserAccess);
		for form in [WriteForm::Create, WriteForm::Update, WriteForm::Upsert, WriteForm::Delete] {
			assert_eq!(
				refused(form),
				[
					tenant.clone(),
					gate(GateInstr::JumpIfUnchecked {
						action: GateAction::Edit,
						target: 3,
					}),
					Instr::Jump {
						target: 3,
					},
					Instr::EndRow,
				],
				"{form:?}"
			);
		}
		for form in [WriteForm::Insert, WriteForm::Relate] {
			assert_eq!(refused(form), [tenant.clone(), Instr::EndRow], "{form:?}");
		}
		// A table that is not defined holds no record to refuse, and a `FULL` clause refuses none.
		for clause in [GateClause::Absent, GateClause::Full] {
			assert_eq!(
				gate_code(&gated(WriteForm::Delete, clause), WriteStageKind::CheckRecordUserAccess),
				[tenant.clone(), Instr::EndRow]
			);
		}
	}

	#[test]
	fn a_clause_the_write_does_not_state_is_never_read_as_full() {
		for stage in [
			WriteStageKind::CheckCreatePermissions,
			WriteStageKind::CheckSelectPermissions(RecordImage::Initial),
			WriteStageKind::CheckPermissionsQuickCreate,
			WriteStageKind::RecheckUpdatePermissions,
			WriteStageKind::CheckRecordUserAccess,
		] {
			assert!(
				matches!(
					emitted(&gated(WriteForm::Create, GateClause::Dynamic), stage),
					Err(IrError::NotYet { .. })
				),
				"{stage:?}"
			);
			assert!(
				matches!(
					emitted(&gated(WriteForm::Create, GateClause::LateBound), stage),
					Err(IrError::LateBind { .. })
				),
				"{stage:?}"
			);
			assert!(
				matches!(
					emitted(&gated(WriteForm::Create, GateClause::Unlowerable), stage),
					Err(IrError::Invariant { .. })
				),
				"{stage:?}"
			);
		}
	}

	fn shown(image: RecordImage, checked: bool) -> Instr {
		Instr::Write(WriteInstr::ShowImage {
			image,
			view: None,
			checked,
		})
	}

	fn set(output: WriteOutput) -> Instr {
		Instr::Write(WriteInstr::SetOutput {
			output,
		})
	}

	fn import(target: Label) -> Instr {
		Instr::Write(WriteInstr::JumpIfImport {
			target,
		})
	}

	#[test]
	fn a_shown_output_shows_the_images_it_names_through_a_plain_view() {
		let reduced = Instr::Write(WriteInstr::ClearCurrentReduced);
		for checked in [true, false] {
			let under = |output| StageRun {
				output,
				checked,
				..run()
			};
			let code = |output| {
				emitted(&under(output), WriteStageKind::OutputWrite).expect("a plain view shows")
			};
			assert_eq!(
				code(OutputShape::After),
				[
					import(4),
					reduced.clone(),
					shown(RecordImage::Current, checked),
					set(WriteOutput::Shown(RecordImage::Current)),
					Instr::EndRow,
				]
			);
			assert_eq!(
				code(OutputShape::Before),
				[
					import(4),
					reduced.clone(),
					shown(RecordImage::Initial, checked),
					set(WriteOutput::Shown(RecordImage::Initial)),
					Instr::EndRow,
				]
			);
			assert_eq!(
				code(OutputShape::Diff),
				[
					import(5),
					reduced.clone(),
					shown(RecordImage::Initial, checked),
					shown(RecordImage::Current, checked),
					set(WriteOutput::Diff),
					Instr::EndRow,
				]
			);
		}
	}

	#[test]
	fn an_unspecified_return_is_the_post_image_on_the_orders_that_leave_one() {
		assert_eq!(
			emitted(
				&StageRun {
					output: OutputShape::Default,
					..run()
				},
				WriteStageKind::OutputWrite
			)
			.expect("a plain view shows"),
			[
				import(7),
				Instr::Write(WriteInstr::ClearCurrentReduced),
				Instr::Write(WriteInstr::JumpIfNoPostImage {
					target: 6
				}),
				shown(RecordImage::Current, true),
				set(WriteOutput::Shown(RecordImage::Current)),
				Instr::Jump {
					target: 7
				},
				set(WriteOutput::None),
				Instr::EndRow,
			]
		);
	}

	/// The run of a table `person` whose view is `view`, stating `layers` for it.
	fn layered(view: ImageView, layers: ImageLayers, checked: bool) -> StageRun {
		StageRun {
			checked,
			stored: WriteStored {
				image_layers: layers,
				..storing(Stored::Nothing)
			},
			..viewed(view)
		}
	}

	/// One layer with the column `c`, over an input that declares `c` `COMPUTED`.
	fn computed_layer() -> SubscriberLayer {
		SubscriberLayer {
			fields: FieldsShape::Select(Box::new([FieldShape::Single {
				alias: Some(OutputPath::field("c")),
				kind: None,
			}])),
			computed: vec![Strand::new("c")],
			judged: None,
		}
	}

	/// The regions a write hosts for the layer [`computed_layer`] states: the read of `c`, then
	/// its expression `a`, derived in a computed frame.
	fn computed_layer_regions(b: &mut Builder) -> Vec<RegionId> {
		let owner = b.add_logical(
			LogOp::WriteStage {
				stage: WriteStageKind::OutputWrite,
				mark: crate::exec::ir::logical::ops::WriteMark::Apply,
			},
			&[],
		);
		b.region_with_args(
			owner,
			RegionKind::Clause(ClauseRole::Projection),
			&[TypeKnowledge::Unknown],
			|b, args| {
				b.add(
					ExprOp::Field {
						name: Strand::new("c"),
					},
					&[args[0]],
				)
			},
		);
		let frame = b.frame(EvalFrame::computed(None));
		b.in_frame(frame, |b| {
			b.region_with_args(
				owner,
				RegionKind::FieldFrame(FieldRole::Computed),
				&[timestamp_arg(), TypeKnowledge::Unknown],
				|b, args| {
					b.add(
						ExprOp::Field {
							name: Strand::new("a"),
						},
						&[args[1]],
					)
				},
			);
		});
		b.ir.op(owner).expect("the op").regions.to_vec()
	}

	fn shown_through(image: RecordImage, view: u16, checked: bool) -> Instr {
		Instr::Write(WriteInstr::ShowImage {
			image,
			view: Some(view),
			checked,
		})
	}

	#[test]
	fn a_view_that_would_cut_an_image_is_never_shown_whole() {
		let after = |view, checked, layers| {
			emitted(&layered(view, layers, checked), WriteStageKind::OutputWrite)
		};
		// A view that cuts or derives and states no layers is an invariant break: the image is
		// never shown whole in its place.
		for (view, checked) in
			[(ImageView::Cut, true), (ImageView::Derived, true), (ImageView::Derived, false)]
		{
			assert!(
				matches!(
					after(view, checked, ImageLayers::Unstated),
					Err(IrError::Invariant { .. })
				),
				"{view:?} checked {checked}"
			);
		}
		// A cut field belongs to a caller whose reads are checked; for any other the view is plain.
		assert_eq!(
			after(ImageView::Cut, false, ImageLayers::Unstated)
				.expect("an unchecked caller's view is plain")[2],
			shown(RecordImage::Current, false)
		);
		// A stack of no layer is a view that changes nothing for the principal.
		for checked in [true, false] {
			assert_eq!(
				after(ImageView::Derived, checked, ImageLayers::Stack(Vec::new()))
					.expect("a view with no layer is plain")[2],
				shown(RecordImage::Current, checked)
			);
		}
		assert!(matches!(
			after(ImageView::Dynamic, true, ImageLayers::Unstated),
			Err(IrError::NotYet { .. })
		));
		assert!(matches!(
			after(ImageView::LateBound, true, ImageLayers::Unstated),
			Err(IrError::LateBind { .. })
		));
	}

	/// The layers a write states are one host pipeline over the image, which every stage that shows
	/// or reads an image through the view shares.
	#[test]
	fn the_layers_of_a_view_are_one_host_pipeline_over_the_image() {
		let mut b = Builder::new();
		let regions = computed_layer_regions(&mut b);
		let run = StageRun {
			layers: vec![regions],
			..layered(ImageView::Derived, ImageLayers::Stack(vec![computed_layer()]), true)
		};
		let (code, hosts) =
			emitted_in_context(&b.ir, &run, WriteStageKind::OutputWrite).expect("the view lowers");
		assert_eq!(code[2], shown_through(RecordImage::Current, 0, true));
		assert_eq!(hosts.len(), 1);

		let mut em = Emitter::new();
		let mut units = LowerCx::detached();
		let mut cx = Stages::new(
			&run,
			&b.ir,
			LowerLimits::default(),
			crate::exec::ir::analysis::cx::builtin_registry(),
		)
		.in_context(&mut units);
		let first = cx.shown_view(&mut em).expect("the view lowers");
		assert_eq!(first, (Some(0), true));
		assert_eq!(
			cx.snapshot(&mut em).expect("the snapshot reads the same view"),
			DataInstr::Snapshot {
				view: Some(0),
				bare: None,
				checked: true,
			}
		);
		let program = em
			.finish_program(Vec::new(), crate::exec::AccessMode::ReadOnly)
			.expect("the program finishes");
		assert_eq!(program.host_code.len(), 1, "the view is lowered once for every stage");
	}

	/// One layer with the column `s`, a guard on a stored field.
	fn guarded_layer() -> SubscriberLayer {
		SubscriberLayer {
			fields: FieldsShape::Select(Box::new([FieldShape::Single {
				alias: Some(OutputPath::field("s")),
				kind: None,
			}])),
			computed: Vec::new(),
			judged: None,
		}
	}

	/// The regions a write hosts for the layer [`guarded_layer`] states: the read of `s`, then
	/// its value, judged in a permission frame.
	fn guarded_layer_regions(b: &mut Builder) -> Vec<RegionId> {
		let owner = b.add_logical(
			LogOp::WriteStage {
				stage: WriteStageKind::OutputWrite,
				mark: crate::exec::ir::logical::ops::WriteMark::Apply,
			},
			&[],
		);
		b.region_with_args(
			owner,
			RegionKind::Clause(ClauseRole::Projection),
			&[TypeKnowledge::Unknown],
			|b, args| {
				b.add(
					ExprOp::Field {
						name: Strand::new("s"),
					},
					&[args[0]],
				)
			},
		);
		let frame = b.frame(EvalFrame::permission(None));
		b.in_frame(frame, |b| {
			b.region_with_args(
				owner,
				RegionKind::FieldFrame(FieldRole::Computed),
				&[timestamp_arg(), TypeKnowledge::Unknown],
				|b, args| {
					b.add(
						ExprOp::Field {
							name: Strand::new("s"),
						},
						&[args[1]],
					)
				},
			);
		});
		b.ir.op(owner).expect("the op").regions.to_vec()
	}

	/// A checked caller's snapshot of a record with no pre-image is cut by the view's guards on
	/// stored fields, lowered once beside the view; a view that only derives guards none.
	#[test]
	fn a_checked_snapshot_without_a_pre_image_is_cut_by_the_stored_guards() {
		let mut b = Builder::new();
		let derived = computed_layer_regions(&mut b);
		let guarded = guarded_layer_regions(&mut b);
		let run = StageRun {
			layers: vec![derived, guarded],
			..layered(
				ImageView::Derived,
				ImageLayers::Stack(vec![computed_layer(), guarded_layer()]),
				true,
			)
		};
		let mut em = Emitter::new();
		let mut units = LowerCx::detached();
		let mut cx = Stages::new(
			&run,
			&b.ir,
			LowerLimits::default(),
			crate::exec::ir::analysis::cx::builtin_registry(),
		)
		.in_context(&mut units);
		let snapshot = cx.snapshot(&mut em).expect("the snapshot lowers");
		assert_eq!(
			snapshot,
			DataInstr::Snapshot {
				view: Some(0),
				bare: Some(1),
				checked: true,
			}
		);
		assert_eq!(cx.snapshot(&mut em).expect("the snapshot lowers"), snapshot);
		let program = em
			.finish_program(Vec::new(), crate::exec::AccessMode::ReadOnly)
			.expect("the program finishes");
		assert_eq!(program.host_code.len(), 2, "the view and its guards are lowered once each");

		let unchecked = StageRun {
			checked: false,
			..run.clone()
		};
		let mut em = Emitter::new();
		let mut units = LowerCx::detached();
		let mut cx = Stages::new(
			&unchecked,
			&b.ir,
			LowerLimits::default(),
			crate::exec::ir::analysis::cx::builtin_registry(),
		)
		.in_context(&mut units);
		assert!(matches!(
			cx.snapshot(&mut em).expect("the snapshot lowers"),
			DataInstr::Snapshot {
				bare: None,
				..
			}
		));
	}

	/// A view with no layer form has the host put an image through the table's declarations.
	#[test]
	fn a_view_with_no_layer_form_is_enforced_by_the_host() {
		let run = layered(ImageView::Cut, ImageLayers::Enforced, true);
		let (code, hosts) = emitted_in(&Builder::new().ir, &run, WriteStageKind::OutputWrite)
			.expect("the view lowers");
		assert_eq!(code[2], shown_through(RecordImage::Current, 0, true));
		assert_eq!(hosts.len(), 1);
		assert_eq!(
			hosts[0].enforce,
			Some(Enforce::Image(surrealdb_strand::TableName::new(surrealdb_strand::Strand::new(
				"person"
			))))
		);
	}

	/// The `Clause(ReturnClause)` region of a write, which takes the post-image and the pre-image
	/// and is built by `body`, which ends the block. Answers the region.
	fn return_region(
		b: &mut Builder,
		body: impl FnOnce(&mut Builder, ValueId, ValueId),
	) -> RegionId {
		let owner = b.add_logical(
			LogOp::WriteStage {
				stage: WriteStageKind::OutputWrite,
				mark: crate::exec::ir::logical::ops::WriteMark::Apply,
			},
			&[],
		);
		b.open_with(
			owner,
			RegionKind::Clause(ClauseRole::ReturnClause),
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			|b, args| body(b, args[0], args[1]),
		);
		*b.ir.op(owner).and_then(|data| data.regions.last()).expect("the stage hosts the region")
	}

	/// A write returning fields over the region `region`, whose records are `form`.
	fn returning(region: RegionId, form: WriteForm) -> StageRun {
		StageRun {
			output: OutputShape::Fields,
			returns: Some(region),
			form,
			..run()
		}
	}

	/// The projection op of `shape` over `row`, with `columns` column regions that each yield what
	/// `column` builds.
	fn projected(
		b: &mut Builder,
		shape: ProjectionShape,
		row: ValueId,
		columns: usize,
		column: impl Fn(&mut Builder) -> ValueId,
	) -> ValueId {
		let project = b.add_phys_value(PhysOp::Project(Box::new(shape)), &[row]);
		for _ in 0..columns {
			b.region(project, RegionKind::Clause(ClauseRole::Projection), &column);
		}
		b.result_of(project)
	}

	fn named(output: &str) -> ProjectedFieldShape {
		ProjectedFieldShape {
			output: FieldPath(vec![FieldPathPart::Field(output.to_owned())]),
			aliased: true,
			sort_key: None,
		}
	}

	#[test]
	fn a_return_of_the_image_alone_sets_the_post_image_after_showing_both() {
		let mut b = Builder::new();
		let alone = return_region(&mut b, |b, row, _| {
			b.terminate(ExprOp::Yield, &[row]);
		});
		let every = return_region(&mut b, |b, row, _| {
			let all = projected(b, ProjectionShape::All, row, 0, |b| b.literal(Value::None));
			b.terminate(ExprOp::Yield, &[all]);
		});
		for region in [alone, every] {
			let (code, hosts) = emitted_in(
				&b.ir,
				&returning(region, WriteForm::Update),
				WriteStageKind::OutputWrite,
			)
			.expect("it lowers");
			assert_eq!(
				code,
				[
					Instr::Write(WriteInstr::JumpIfImport {
						target: 5
					}),
					Instr::Write(WriteInstr::ClearCurrentReduced),
					shown(RecordImage::Initial, true),
					shown(RecordImage::Current, true),
					set(WriteOutput::Shown(RecordImage::Current)),
					Instr::EndRow,
				]
			);
			assert!(hosts.is_empty(), "the image itself needs no pipeline");
		}
	}

	#[test]
	fn a_return_of_fields_is_a_host_pipeline_over_the_post_image_reading_the_pre_image_as_a_param()
	{
		let mut b = Builder::new();
		let region = return_region(&mut b, |b, row, _| {
			let object = projected(
				b,
				ProjectionShape::Fields {
					fields: vec![named("x")],
					omit: Vec::new(),
				},
				row,
				1,
				|b| b.literal(Value::from(7_i64)),
			);
			b.terminate(ExprOp::Yield, &[object]);
		});
		let (code, hosts) =
			emitted_in(&b.ir, &returning(region, WriteForm::Update), WriteStageKind::OutputWrite)
				.expect("it lowers");
		assert_eq!(
			&code[2..],
			[
				shown(RecordImage::Initial, true),
				shown(RecordImage::Current, true),
				set(WriteOutput::Projection(0)),
				Instr::EndRow,
			]
		);
		let [projection] = hosts.as_slice() else {
			panic!("the projection is one host pipeline");
		};
		assert!(matches!(projection.source, Source::Once));
		assert!(matches!(projection.code.first(), Some(Instr::NewObject { .. })));
		let tail = &projection.code[projection.code.len() - 2..];
		assert!(matches!(tail, [Instr::Emit { .. }, Instr::EndRow]));
		assert!(
			!projection.code.iter().any(|instr| matches!(instr, Instr::LoadParam { .. })),
			"a pre-image nothing reads is not loaded"
		);
	}

	#[test]
	fn a_column_reading_the_pre_image_loads_it_as_the_param_the_host_binds() {
		let mut b = Builder::new();
		let region = return_region(&mut b, |b, row, before| {
			let object = {
				let project = b.add_phys_value(
					PhysOp::Project(Box::new(ProjectionShape::Fields {
						fields: vec![named("was")],
						omit: Vec::new(),
					})),
					&[row],
				);
				b.region(project, RegionKind::Clause(ClauseRole::Projection), |_| before);
				b.result_of(project)
			};
			b.terminate(ExprOp::Yield, &[object]);
		});
		let (_, hosts) =
			emitted_in(&b.ir, &returning(region, WriteForm::Update), WriteStageKind::OutputWrite)
				.expect("it lowers");
		let [projection] = hosts.as_slice() else {
			panic!("the projection is one host pipeline");
		};
		assert!(matches!(
			projection.code.first(),
			Some(Instr::LoadParam {
				frame: None,
				..
			})
		));
	}

	#[test]
	fn a_star_over_the_image_a_delete_leaves_tests_the_image() {
		let mut b = Builder::new();
		let region = return_region(&mut b, |b, row, _| {
			let object = projected(
				b,
				ProjectionShape::Star {
					fields: vec![named("x")],
					omit: Vec::new(),
				},
				row,
				1,
				|b| b.literal(Value::from(1_i64)),
			);
			b.terminate(ExprOp::Yield, &[object]);
		});
		assert!(
			emitted_in(&b.ir, &returning(region, WriteForm::Update), WriteStageKind::OutputWrite)
				.is_ok()
		);
		// The image a delete leaves may not be an object, so the projection tests it first.
		let (_, hosts) =
			emitted_in(&b.ir, &returning(region, WriteForm::Delete), WriteStageKind::OutputWrite)
				.expect("it lowers");
		assert!(
			hosts.iter().any(|host| host
				.code
				.iter()
				.any(|instr| matches!(instr, Instr::JumpIfNotObject { .. }))),
			"{hosts:?}"
		);
	}

	#[test]
	fn a_return_of_fields_over_a_view_that_states_no_layers_is_refused_before_anything_is_emitted()
	{
		let mut b = Builder::new();
		let region = return_region(&mut b, |b, row, _| {
			b.terminate(ExprOp::Yield, &[row]);
		});
		let run = StageRun {
			image: WriteImage {
				view: ImageView::Derived,
				keyed: false,
				id_default: false,
			},
			..returning(region, WriteForm::Update)
		};
		assert!(matches!(
			emitted_in(&b.ir, &run, WriteStageKind::OutputWrite),
			Err(IrError::Invariant { .. })
		));
	}

	#[test]
	fn the_special_fields_are_stamped_and_a_keyed_table_judges_the_key() {
		let keyed = |keyed| StageRun {
			image: WriteImage {
				view: ImageView::Plain,
				keyed,
				id_default: false,
			},
			..run()
		};
		assert_eq!(
			emitted(&keyed(false), WriteStageKind::DefaultRecordData).expect("it lowers"),
			[data(DataInstr::StampRecordFields), Instr::EndRow]
		);
		assert_eq!(
			emitted(&keyed(true), WriteStageKind::DefaultRecordData).expect("it lowers"),
			[data(DataInstr::StampRecordFields), data(DataInstr::SettleKeyFields), Instr::EndRow]
		);
		assert_eq!(
			emitted(&keyed(true), WriteStageKind::GenerateRecordId).expect("it lowers"),
			[
				data(DataInstr::GenerateId {
					default: None
				}),
				Instr::EndRow
			]
		);
		for stage in [WriteStageKind::DefaultRecordData, WriteStageKind::GenerateRecordId] {
			assert!(matches!(
				emitted(&viewed(ImageView::Dynamic), stage),
				Err(IrError::NotYet { .. })
			));
			assert!(matches!(
				emitted(&viewed(ImageView::LateBound), stage),
				Err(IrError::LateBind { .. })
			));
		}
	}

	/// A stage op of `stage`, whose region of `kind` takes the snapshot as its row and is built by
	/// `body`, which ends the block. Answers the region.
	fn clause_region(
		b: &mut Builder,
		stage: WriteStageKind,
		kind: RegionKind,
		body: impl FnOnce(&mut Builder, ValueId),
	) -> RegionId {
		let owner = b.add_logical(
			LogOp::WriteStage {
				stage,
				mark: crate::exec::ir::logical::ops::WriteMark::Apply,
			},
			&[],
		);
		b.open_with(owner, kind, &[TypeKnowledge::Unknown], |b, args| body(b, args[0]));
		*b.ir.op(owner).and_then(|data| data.regions.last()).expect("the stage hosts the region")
	}

	fn assign_to(b: &mut Builder, field: &str, value: ValueId) {
		b.add_phys(
			PhysOp::Assign(Box::new(AssignAttrs {
				place: Place {
					parts: vec![PlacePart::Field(Strand::new(field))].into_boxed_slice(),
				},
				op: AssignOperator::Assign,
			})),
			&[value],
		);
	}

	#[test]
	fn a_condition_is_judged_over_the_snapshot_and_a_rejected_record_is_skipped() {
		let mut b = Builder::new();
		let region = clause_region(
			&mut b,
			WriteStageKind::CheckWhereCondition,
			RegionKind::Clause(ClauseRole::WriteCond),
			|b, row| {
				let one = b.literal(Value::from(1_i64));
				let sum = b.binary(BinaryOperator::Add, row, one);
				b.terminate(ExprOp::Yield, &[sum]);
			},
		);
		let run = StageRun {
			cond: Some(region),
			..run()
		};
		let (code, hosts) =
			emitted_in(&b.ir, &run, WriteStageKind::CheckWhereCondition).expect("it lowers");
		assert_eq!(
			code,
			[
				data(DataInstr::Snapshot {
					view: None,
					bare: None,
					checked: true
				}),
				data(DataInstr::Where {
					pipeline: 0,
					skip: 2
				}),
				Instr::EndRow,
			]
		);
		let [condition] = hosts.as_slice() else {
			panic!("the condition is one host pipeline");
		};
		assert!(matches!(condition.source, Source::Once));
		let tail = &condition.code[condition.code.len() - 2..];
		assert!(matches!(tail, [Instr::Emit { .. }, Instr::EndRow]));
	}

	#[test]
	fn a_set_clause_evaluates_each_value_in_written_order_over_one_snapshot() {
		let mut b = Builder::new();
		let region = clause_region(
			&mut b,
			WriteStageKind::ComputeInputData,
			RegionKind::Clause(ClauseRole::SetValue),
			|b, row| {
				let one = b.literal(Value::from(1_i64));
				assign_to(b, "a", one);
				let next = b.binary(BinaryOperator::Add, row, one);
				assign_to(b, "b", next);
				b.terminate(ExprOp::Yield, &[]);
			},
		);
		assert_eq!(
			clause_kind(&b.ir, region, ClauseRole::SetValue).expect("a set"),
			ClauseKind::Set
		);
		for (role, binds_input) in
			[(ClauseRole::SetValue, false), (ClauseRole::DuplicateUpdate, true)]
		{
			let run = StageRun {
				data: Some(DataClause {
					region,
					role,
					kind: ClauseKind::Set,
				}),
				..run()
			};
			let (code, hosts) =
				emitted_in(&b.ir, &run, WriteStageKind::ComputeInputData).expect("it lowers");
			assert_eq!(
				code,
				[
					data(DataInstr::JumpIfInput {
						target: 3
					}),
					data(DataInstr::Snapshot {
						view: None,
						bare: None,
						checked: true
					}),
					data(DataInstr::ComputeInput {
						pipeline: Some(0),
						binds_input
					}),
					Instr::EndRow,
				]
			);
			let [values] = hosts.as_slice() else {
				panic!("the clause is one host pipeline");
			};
			assert!(matches!(values.code.first(), Some(Instr::NewArray { .. })));
			let pushes =
				values.code.iter().filter(|instr| matches!(instr, Instr::ArrayPush { .. })).count();
			assert_eq!(pushes, 2, "one push per assignment");
		}
	}

	#[test]
	fn a_set_clause_whose_value_throws_raises_in_its_host_pipeline() {
		let mut b = Builder::new();
		let region = clause_region(
			&mut b,
			WriteStageKind::ComputeInputData,
			RegionKind::Clause(ClauseRole::SetValue),
			|b, _| {
				let thrown = b.literal(Value::from("boom"));
				b.terminate(ExprOp::Throw, &[thrown]);
			},
		);
		let run = StageRun {
			data: Some(DataClause {
				region,
				role: ClauseRole::SetValue,
				kind: ClauseKind::Set,
			}),
			..run()
		};
		let (_, hosts) =
			emitted_in(&b.ir, &run, WriteStageKind::ComputeInputData).expect("it lowers");
		let [values] = hosts.as_slice() else {
			panic!("the clause is one host pipeline");
		};
		assert!(values.code.iter().any(|instr| matches!(instr, Instr::Throw { .. })));
		assert!(!values.code.iter().any(|instr| matches!(instr, Instr::ArrayPush { .. })));
	}

	#[test]
	fn a_content_clause_whose_value_throws_raises_in_its_host_pipeline() {
		let mut b = Builder::new();
		let region = clause_region(
			&mut b,
			WriteStageKind::ComputeInputData,
			RegionKind::Clause(ClauseRole::Content),
			|b, _| {
				let thrown = b.literal(Value::from("boom"));
				b.terminate(ExprOp::Throw, &[thrown]);
			},
		);
		let kind = clause_kind(&b.ir, region, ClauseRole::Content).expect("a raising content");
		let run = StageRun {
			data: Some(DataClause {
				region,
				role: ClauseRole::Content,
				kind,
			}),
			..run()
		};
		let (_, hosts) =
			emitted_in(&b.ir, &run, WriteStageKind::ComputeInputData).expect("it lowers");
		let [value] = hosts.as_slice() else {
			panic!("the clause is one host pipeline");
		};
		assert!(value.code.iter().any(|instr| matches!(instr, Instr::Throw { .. })));
	}

	#[test]
	fn a_duplicate_update_reads_the_supplied_value_as_its_input_parameter() {
		let mut b = Builder::new();
		let owner = b.add_logical(
			LogOp::WriteStage {
				stage: WriteStageKind::ComputeInputData,
				mark: crate::exec::ir::logical::ops::WriteMark::Apply,
			},
			&[],
		);
		let kind = RegionKind::Clause(ClauseRole::DuplicateUpdate);
		b.open_with(owner, kind, &[TypeKnowledge::Unknown, TypeKnowledge::Unknown], |b, args| {
			assign_to(b, "a", args[1]);
			b.terminate(ExprOp::Yield, &[]);
		});
		let region =
			*b.ir.op(owner).and_then(|data| data.regions.last()).expect("the stage hosts it");
		let run = StageRun {
			data: Some(DataClause {
				region,
				role: ClauseRole::DuplicateUpdate,
				kind: ClauseKind::Set,
			}),
			..run()
		};
		let (_, hosts) =
			emitted_in(&b.ir, &run, WriteStageKind::ComputeInputData).expect("it lowers");
		let [values] = hosts.as_slice() else {
			panic!("the clause is one host pipeline");
		};
		assert!(values.code.iter().any(|instr| matches!(instr, Instr::LoadParam { .. })));
	}

	#[test]
	fn a_content_clause_stores_the_one_value_and_an_unset_evaluates_nothing() {
		let mut b = Builder::new();
		let region = clause_region(
			&mut b,
			WriteStageKind::ComputeInputData,
			RegionKind::Clause(ClauseRole::Content),
			|b, _| {
				let value = b.literal(Value::from(1_i64));
				b.add_phys(
					PhysOp::Content {
						mode: ContentMode::Merge,
					},
					&[value],
				);
				b.terminate(ExprOp::Yield, &[]);
			},
		);
		assert_eq!(
			clause_kind(&b.ir, region, ClauseRole::Content).expect("a merge"),
			ClauseKind::Content(ContentMode::Merge)
		);
		let content = StageRun {
			data: Some(DataClause {
				region,
				role: ClauseRole::Content,
				kind: ClauseKind::Content(ContentMode::Merge),
			}),
			..run()
		};
		let (_, hosts) =
			emitted_in(&b.ir, &content, WriteStageKind::ComputeInputData).expect("it lowers");
		let [value] = hosts.as_slice() else {
			panic!("the clause is one host pipeline");
		};
		assert!(!value.code.iter().any(|instr| matches!(instr, Instr::NewArray { .. })));

		let unset = StageRun {
			data: Some(DataClause {
				region,
				role: ClauseRole::Content,
				kind: ClauseKind::Unset,
			}),
			..run()
		};
		let (code, hosts) =
			emitted_in(&b.ir, &unset, WriteStageKind::ComputeInputData).expect("it lowers");
		assert!(hosts.is_empty());
		assert!(code.contains(&data(DataInstr::ComputeInput {
			pipeline: None,
			binds_input: false
		})));
	}

	#[test]
	fn a_clause_that_says_two_things_or_an_inserts_rows_are_no_data_clause() {
		let mut b = Builder::new();
		let region = clause_region(
			&mut b,
			WriteStageKind::ComputeInputData,
			RegionKind::Clause(ClauseRole::Content),
			|b, _| {
				let value = b.literal(Value::from(1_i64));
				b.add_logical(
					LogOp::Content {
						mode: ContentMode::Single,
					},
					&[value],
				);
				b.terminate(ExprOp::Yield, &[]);
			},
		);
		assert!(matches!(
			clause_kind(&b.ir, region, ClauseRole::Content),
			Err(IrError::Invariant { .. })
		));
		let mut b = Builder::new();
		let region = clause_region(
			&mut b,
			WriteStageKind::ComputeInputData,
			RegionKind::Clause(ClauseRole::SetValue),
			|b, _| {
				let value = b.literal(Value::from(1_i64));
				assign_to(b, "a", value);
				b.add_phys(
					PhysOp::Content {
						mode: ContentMode::Merge,
					},
					&[value],
				);
				b.terminate(ExprOp::Yield, &[]);
			},
		);
		assert!(matches!(clause_kind(&b.ir, region, ClauseRole::SetValue), Err(IrError::Stale(_))));
	}

	#[test]
	fn a_patch_is_applied_through_the_snapshot_and_any_other_clause_as_evaluated() {
		let patch = StageRun {
			data: Some(DataClause {
				region: RegionId::default(),
				role: ClauseRole::Content,
				kind: ClauseKind::Content(ContentMode::Patch),
			}),
			..run()
		};
		assert_eq!(
			emitted(&patch, WriteStageKind::ProcessRecordData).expect("it lowers"),
			[
				data(DataInstr::ApplyPatch {
					view: None,
					bare: None,
					checked: true
				}),
				Instr::EndRow
			]
		);
		let merge = StageRun {
			data: Some(DataClause {
				region: RegionId::default(),
				role: ClauseRole::Content,
				kind: ClauseKind::Content(ContentMode::Merge),
			}),
			..run()
		};
		assert_eq!(
			emitted(&merge, WriteStageKind::ProcessRecordData).expect("it lowers"),
			[data(DataInstr::ApplyInput), Instr::EndRow]
		);
	}

	#[test]
	fn a_snapshot_a_view_would_cut_is_never_read_whole() {
		let mut b = Builder::new();
		let region = clause_region(
			&mut b,
			WriteStageKind::CheckWhereCondition,
			RegionKind::Clause(ClauseRole::WriteCond),
			|b, row| {
				b.terminate(ExprOp::Yield, &[row]);
			},
		);
		for view in [ImageView::Cut, ImageView::Derived] {
			let run = StageRun {
				cond: Some(region),
				..viewed(view)
			};
			assert!(
				matches!(
					emitted_in_context(&b.ir, &run, WriteStageKind::CheckWhereCondition),
					Err(IrError::Invariant { .. })
				),
				"{view:?} of a checked caller has no snapshot"
			);
		}
		// A caller whose reads are not checked reads the snapshot of a table that only cuts.
		let run = StageRun {
			cond: Some(region),
			checked: false,
			..viewed(ImageView::Cut)
		};
		let (code, _) = emitted_in_context(&b.ir, &run, WriteStageKind::CheckWhereCondition)
			.expect("it lowers");
		assert_eq!(
			code[0],
			data(DataInstr::Snapshot {
				view: None,
				bare: None,
				checked: false
			})
		);
		// A view that has layers is read through them: the snapshot is the layers' pipeline, then
		// the condition's.
		let regions = computed_layer_regions(&mut b);
		let run = StageRun {
			cond: Some(region),
			layers: vec![regions],
			..layered(ImageView::Derived, ImageLayers::Stack(vec![computed_layer()]), true)
		};
		let (code, hosts) = emitted_in_context(&b.ir, &run, WriteStageKind::CheckWhereCondition)
			.expect("it lowers");
		assert_eq!(
			code[0],
			data(DataInstr::Snapshot {
				view: Some(0),
				bare: None,
				checked: true
			})
		);
		assert_eq!(hosts.len(), 2, "the view's pipeline, then the condition's");
	}

	#[test]
	fn a_table_that_stores_nothing_lowers_the_stages_that_maintain_it() {
		let run = stored_run(storing(Stored::Nothing));
		let plan = Arc::new(SchemaPlan {
			table: surrealdb_strand::TableName::new(surrealdb_strand::Strand::new("person")),
			schemafull: true,
			id_field: None,
			fields: Vec::new(),
		});
		let code = |stage| match emitted(&run, stage) {
			Ok(code) => code,
			Err(error) => panic!("{stage:?} lowers: {error}"),
		};
		assert_eq!(
			code(WriteStageKind::ProcessTableFields),
			[
				schema(SchemaInstr::Begin {
					table: Arc::clone(&plan),
					end: 2,
				}),
				schema(SchemaInstr::Finish {
					computed: Arc::from(Vec::new()),
				}),
				Instr::EndRow,
			]
		);
		assert_eq!(
			code(WriteStageKind::CleanupTableFields),
			[
				schema(SchemaInstr::Cleanup {
					table: plan
				}),
				Instr::EndRow
			]
		);
		// A statement that cannot define an index or an event leaves their stages out.
		for stage in [
			WriteStageKind::MaterialiseObservedFields,
			WriteStageKind::ProcessTableReferences,
			WriteStageKind::CleanupTableReferences,
			WriteStageKind::StoreIndexData,
			WriteStageKind::ProcessTableEvents(Action::Create),
		] {
			assert_eq!(code(stage), [Instr::EndRow], "{stage:?}");
		}
		assert_eq!(
			code(WriteStageKind::ProcessTableViews(Action::Update)),
			[
				Instr::Write(WriteInstr::Observe(ObserveInstr::Views {
					action: Action::Update,
					lowered: Arc::new(LoweredViews {
						definitions: Arc::from(Vec::new()),
						views: Vec::new(),
					}),
				})),
				Instr::EndRow,
			]
		);
	}

	#[test]
	fn a_statement_that_may_change_the_catalog_still_checks_the_indexes_of_the_table() {
		let run = stored_run(WriteStored {
			catalog_fixed: false,
			..storing(Stored::Nothing)
		});
		assert_eq!(
			emitted(&run, WriteStageKind::StoreIndexData).expect("it lowers"),
			[
				storage(StorageInstr::JumpIf {
					fact: StorageFact::Unmaintained,
					when: true,
					target: 3,
				}),
				storage(StorageInstr::JumpIf {
					fact: StorageFact::TableDropped,
					when: true,
					target: 3,
				}),
				storage(StorageInstr::CheckIndexes {
					lowered: Arc::new(LoweredIndexes::new(Arc::from(Vec::new()), Vec::new())),
				}),
				Instr::EndRow,
			]
		);
		// Its events are a unit the compile driver plans over the events the table holds when each
		// record is written, so the stage asks for it though the table holds none now.
		let (code, needed) =
			emitted_with_units(&run, WriteStageKind::ProcessTableEvents(Action::Create))
				.expect("it lowers");
		assert_eq!(
			code,
			[
				Instr::Write(WriteInstr::Observe(ObserveInstr::Events {
					unit: UnitId(0),
					depth: 0,
				})),
				Instr::EndRow,
			]
		);
		assert_eq!(needed.len(), 1);
	}

	/// The events of a table that holds some are one unit asked for with the table, the action and
	/// the frame the write is lowered for, whose id the instruction runs. The count the events
	/// resume starts at the depth the context lowers at and the place of the write in the request.
	#[test]
	fn the_events_of_a_table_are_one_unit_asked_for_by_table_action_and_frame() {
		let run = StageRun {
			nesting: StatementNesting {
				arms: 1,
				block: true,
				expression: false,
			},
			..stored_run(storing(Stored::Declared))
		};
		let (code, needed) =
			emitted_with_units(&run, WriteStageKind::ProcessTableEvents(Action::Update))
				.expect("it lowers");
		assert_eq!(
			code,
			[
				Instr::Write(WriteInstr::Observe(ObserveInstr::Events {
					unit: UnitId(0),
					depth: 3,
				})),
				Instr::EndRow,
			]
		);
		assert_eq!(
			needed,
			vec![super::super::units::Needed {
				request: UnitRequest::Events {
					table: surrealdb_strand::TableName::new(surrealdb_strand::Strand::new(
						"person"
					)),
					action: Action::Update,
					frame: LowerCx::detached().frame().clone(),
				},
				id: Some(UnitId(0)),
			}]
		);
	}

	/// The same events for another action are another unit, and the events of a table for one
	/// action are one unit however many stages ask for them.
	#[test]
	fn the_events_of_two_actions_are_two_units_and_of_one_action_are_one() {
		let ir = Builder::new().ir;
		let run = stored_run(storing(Stored::Declared));
		let mut em = Emitter::new();
		let skip = em.label().expect("a label");
		let mut units = LowerCx::detached();
		{
			let mut cx = Stages::new(
				&run,
				&ir,
				LowerLimits::default(),
				crate::exec::ir::analysis::cx::builtin_registry(),
			)
			.in_context(&mut units);
			for action in [Action::Create, Action::Update, Action::Create] {
				emit_stage(&mut em, &mut cx, WriteStageKind::ProcessTableEvents(action), skip)
					.expect("it lowers");
			}
		}
		em.bind(skip).expect("the label binds");
		em.emit(Instr::EndRow);
		let code = em.finish_pipeline(Source::Once).expect("the pipeline ends").code;
		let ids: Vec<UnitId> = code
			.iter()
			.filter_map(|instr| match instr {
				Instr::Write(WriteInstr::Observe(ObserveInstr::Events {
					unit,
					..
				})) => Some(*unit),
				Instr::CoerceField {
					..
				}
				| Instr::BeginComputing {
					..
				}
				| Instr::LandRow {
					..
				}
				| Instr::BindRow {
					..
				}
				| Instr::JumpIfPermissionsBypassed {
					..
				}
				| Instr::CompareFieldInt {
					..
				}
				| Instr::JumpIfFieldInt {
					..
				}
				| Instr::JumpIfCompare {
					..
				}
				| Instr::Compare {
					..
				}
				| Instr::Arith {
					..
				}
				| Instr::Unary {
					..
				}
				| Instr::NewArray {
					..
				}
				| Instr::ArrayPush {
					..
				}
				| Instr::LoadParam {
					..
				}
				| Instr::LoadSlot {
					..
				}
				| Instr::SortKey {
					..
				}
				| Instr::Fetch {
					..
				}
				| Instr::Access {
					..
				}
				| Instr::Each {
					..
				}
				| Instr::Throw {
					..
				}
				| Instr::FilterBegin {
					..
				}
				| Instr::MapBegin {
					..
				}
				| Instr::CollectPush {
					..
				}
				| Instr::Nav {
					..
				}
				| Instr::CallAllowed {
					..
				}
				| Instr::Call {
					..
				}
				| Instr::FunctionEnter {
					..
				}
				| Instr::FunctionBind {
					..
				}
				| Instr::FunctionReturn {
					..
				}
				| Instr::FunctionCall {
					..
				}
				| Instr::ArrayArg {
					..
				}
				| Instr::ElemNext {
					..
				}
				| Instr::ClosureBind {
					..
				}
				| Instr::ClosureReturn {
					..
				}
				| Instr::Dive {
					..
				}
				| Instr::Subquery {
					..
				}
				| Instr::MakeClosure {
					..
				}
				| Instr::ClosureField {
					..
				}
				| Instr::RequireClosure {
					..
				}
				| Instr::Invoke {
					..
				}
				| Instr::Lookup {
					..
				}
				| Instr::Signal {
					..
				}
				| Instr::Extract {
					..
				}
				| Instr::KnnRank {
					..
				}
				| Instr::Measure {
					..
				}
				| Instr::Mock {
					..
				}
				| Instr::Recurse {
					..
				}
				| Instr::Destructure {
					..
				}
				| Instr::RecordId {
					..
				}
				| Instr::MethodResolve {
					..
				}
				| Instr::Method {
					..
				}
				| Instr::MethodClosure {
					..
				}
				| Instr::JumpIfNullish {
					..
				}
				| Instr::JumpIfNone {
					..
				}
				| Instr::JumpIf {
					..
				}
				| Instr::Skip {
					..
				}
				| Instr::Take {
					..
				}
				| Instr::JumpIfLimitReached {
					..
				}
				| Instr::NewObject {
					..
				}
				| Instr::SeedObject {
					..
				}
				| Instr::BuildObject {
					..
				}
				| Instr::LoadPath {
					..
				}
				| Instr::LoadRow {
					..
				}
				| Instr::FetchRow {
					..
				}
				| Instr::RemoveField {
					..
				}
				| Instr::RemovePath {
					..
				}
				| Instr::Omit {
					..
				}
				| Instr::SetField {
					..
				}
				| Instr::WriteProjection {
					..
				}
				| Instr::Load {
					..
				}
				| Instr::Emit {
					..
				}
				| Instr::EmitEnd {
					..
				}
				| Instr::BuildObjectEmit {
					..
				}
				| Instr::FilterFieldInt {
					..
				}
				| Instr::FilterCompare {
					..
				}
				| Instr::Filter {
					..
				}
				| Instr::SplitPush {
					..
				}
				| Instr::SortPush {
					..
				}
				| Instr::Distinct {
					..
				}
				| Instr::JoinProbe {
					..
				}
				| Instr::VerdictJump {
					..
				}
				| Instr::VerdictSet {
					..
				}
				| Instr::JoinPair {
					..
				}
				| Instr::GroupKey {
					..
				}
				| Instr::GroupSelect {
					..
				}
				| Instr::AccUpdate {
					..
				}
				| Instr::Jump {
					..
				}
				| Instr::Next {
					..
				}
				| Instr::Write(_)
				| Instr::Store(_)
				| Instr::Block(_)
				| Instr::EndRow
				| Instr::StopPipeline
				| Instr::TryNone {
					..
				}
				| Instr::TryEnd
				| Instr::MethodRegion {
					..
				}
				| Instr::PermissionRegion {
					..
				}
				| Instr::GroupFirstNeeded {
					..
				}
				| Instr::GroupFirstSet {
					..
				}
				| Instr::CallIndexFn {
					..
				}
				| Instr::Matches {
					..
				}
				| Instr::ExternalEnter {
					..
				}
				| Instr::ExternalCall {
					..
				}
				| Instr::NewSet {
					..
				}
				| Instr::Degree {
					..
				}
				| Instr::RepeatRecurse {
					..
				}
				| Instr::JumpIfNotObject {
					..
				}
				| Instr::JumpIfOpaque {
					..
				}
				| Instr::InvalidControlFlow
				| Instr::JumpIfNotRecordId {
					..
				}
				| Instr::DocumentRoot {
					..
				}
				| Instr::JudgeCopy {
					..
				}
				| Instr::Ir(_) => None,
			})
			.collect();
		assert_eq!(ids, [UnitId(0), UnitId(1), UnitId(0)]);
		assert_eq!(units.take_needed().len(), 2);
	}

	/// A write that is lowered with no unit table cannot ask for the events it runs, and says so
	/// rather than leaving them out.
	#[test]
	fn the_events_of_a_write_lowered_with_no_unit_table_are_an_invariant_break() {
		assert!(matches!(
			emitted(
				&stored_run(storing(Stored::Declared)),
				WriteStageKind::ProcessTableEvents(Action::Create)
			),
			Err(IrError::Invariant { .. })
		));
	}

	/// A write stands at depth `0` as a statement the session runs; an arm of an `IF` that holds
	/// it adds two, and a block, a loop body or an expression that no arm holds adds one.
	#[test]
	fn a_write_stands_as_deep_as_the_arms_and_blocks_that_hold_it() {
		use crate::exec::ir::iface::region_kind::RegionKind as Kind;

		fn nesting(kinds: &[Kind]) -> StatementNesting {
			let mut b = Builder::new();
			let mut owner = b.add_phys(PhysOp::Eval, &[]);
			for kind in kinds {
				b.open(owner, *kind, &[]);
				owner = b.add_phys(PhysOp::Eval, &[]);
			}
			let write = b.ir.op(owner).expect("the write exists").clone();
			statement_nesting(&b.ir, &write)
		}
		fn depth(kinds: &[Kind]) -> u32 {
			nesting(kinds).depth()
		}

		assert_eq!(depth(&[]), 0);
		assert_eq!(depth(&[Kind::Statement(StatementCommit::Auto)]), 0);
		assert_eq!(depth(&[Kind::Block]), 1);
		assert_eq!(depth(&[Kind::StatementFor]), 1);
		assert_eq!(depth(&[Kind::Subquery]), 1);
		assert_eq!(depth(&[Kind::StatementIf]), 2);
		assert_eq!(depth(&[Kind::StatementIf, Kind::StatementIf]), 4);
		assert_eq!(depth(&[Kind::StatementIf, Kind::Block]), 3);
		// Within one container only: a subquery is a unit of its own when it is lowered, and its
		// container does not hold the arm around it.
		assert_eq!(depth(&[Kind::StatementIf, Kind::Subquery]), 2);
		assert_eq!(depth(&[Kind::Block, Kind::Block]), 1);
		// An arm of a value `IF` is an arm, and a value block is a block.
		assert_eq!(depth(&[Kind::ValueIf]), 2);
		assert_eq!(depth(&[Kind::ValueBlock]), 1);
		assert_eq!(depth(&[Kind::ValueIf, Kind::ValueBlock]), 3);
		// A unit stands where the region it is copied from does: the arm around a value block
		// that runs as a unit holds the write in the unit.
		let around = nesting(&[Kind::ValueIf]);
		assert_eq!(around.within(nesting(&[Kind::ValueBlock])).depth(), 3);
		assert_eq!(around.within(nesting(&[Kind::Subquery])).depth(), 2);
		assert_eq!(StatementNesting::default().within(nesting(&[Kind::Subquery])).depth(), 1);
	}

	#[test]
	fn a_table_that_stores_something_or_is_not_stated_is_never_read_as_storing_nothing() {
		let stages = [
			WriteStageKind::ProcessTableFields,
			WriteStageKind::CleanupTableFields,
			WriteStageKind::MaterialiseObservedFields,
			WriteStageKind::ProcessTableReferences,
			WriteStageKind::CleanupTableReferences,
			WriteStageKind::StoreIndexData,
			WriteStageKind::ProcessTableViews(Action::Create),
			WriteStageKind::ProcessTableEvents(Action::Create),
		];
		for stage in stages {
			// The clauses a table that stores some hosts are stated beside the stage, and a table
			// that states none is refused outright.
			assert!(
				matches!(
					emitted(&stored_run(storing(Stored::Declared)), stage),
					Err(IrError::Invariant { .. })
				),
				"{stage:?} of a table that stores some"
			);
			assert!(
				matches!(
					emitted(&stored_run(WriteStored::default()), stage),
					Err(IrError::NotYet { .. })
				),
				"{stage:?} of a write that names no one table"
			);
			assert!(
				matches!(
					emitted(&stored_run(storing(Stored::LateBound)), stage),
					Err(IrError::LateBind { .. })
				),
				"{stage:?} of definitions read before a catalog change"
			);
		}
	}

	// ----- The field schema ---------------------------------------------------------------------

	fn field_stage_owner(b: &mut Builder, stage: WriteStageKind) -> OpId {
		b.add_logical(
			LogOp::WriteStage {
				stage,
				mark: crate::exec::ir::logical::ops::WriteMark::Apply,
			},
			&[],
		)
	}

	/// A region of `kind` hosted by `owner` under `frame`: the clause of a field's place when it is
	/// not a computed body, which takes the row and the place's value, old value and supplied value
	/// after the timestamp. It yields the place's value when `reads_value` and a constant
	/// otherwise.
	fn field_region(
		b: &mut Builder,
		owner: OpId,
		kind: RegionKind,
		frame: EvalFrame,
		reads_value: bool,
	) -> RegionId {
		let places = kind != RegionKind::FieldFrame(FieldRole::Computed);
		let args = vec![
			TypeKnowledge::Unknown;
			if places {
				5
			} else {
				2
			}
		];
		let id = b.frame(frame);
		b.in_frame(id, |b| {
			b.region_with_args(owner, kind, &args, |b, args| match reads_value {
				true => args[2],
				false => b.literal(Value::from(1_i64)),
			});
		});
		*b.ir.op(owner).expect("the stage op").regions.last().expect("a region")
	}

	/// A field that states no clause and permits every change.
	fn bare(index: u16) -> FieldClauses {
		FieldClauses {
			index,
			default: None,
			value: None,
			assert: None,
			create: FieldGate::Allow,
			update: FieldGate::Allow,
		}
	}

	const UNBOUND: FieldClause = FieldClause {
		caller_bound: false,
	};

	/// A run over the `SCHEMAFULL` table `person` that declares the fields `names`, `computed` of
	/// them `COMPUTED`, whose schema states `steps` and whose stage hosts `fields`.
	fn schema_run(
		steps: Vec<FieldStep>,
		names: &[&str],
		computed: &[&str],
		fields: FieldRegions,
	) -> StageRun {
		StageRun {
			stored: WriteStored {
				schema: WriteSchema {
					fields: names.iter().map(|name| Strand::new(*name)).collect(),
					steps: steps.into(),
					computed: computed.iter().map(|name| Strand::new(*name)).collect(),
					..WriteSchema::default()
				},
				..storing(Stored::Declared)
			},
			fields,
			..run()
		}
	}

	/// The plan the instructions of `person` carry, over the fields `names`.
	fn plan_of(names: &[&str]) -> Arc<SchemaPlan> {
		Arc::new(SchemaPlan {
			table: surrealdb_strand::TableName::new(Strand::new("person")),
			schemafull: true,
			id_field: None,
			fields: names
				.iter()
				.map(|name| FieldPlan {
					name: (*name).to_owned(),
				})
				.collect(),
		})
	}

	fn permits_all() -> FieldWrite {
		FieldWrite {
			create: FieldPermission::Allow,
			update: FieldPermission::Allow,
		}
	}

	#[test]
	fn a_field_with_no_clause_is_coerced_and_landed_at_each_of_its_places() {
		let b = Builder::new();
		let run = schema_run(vec![FieldStep::Field(bare(0))], &["a"], &[], FieldRegions::default());
		let (code, program) =
			emitted_program(&b.ir, &run, WriteStageKind::ProcessTableFields).expect("it lowers");
		assert_eq!(
			code,
			[
				schema(SchemaInstr::Begin {
					table: plan_of(&["a"]),
					end: 8
				}),
				schema(SchemaInstr::Field {
					field: 0,
					write: permits_all()
				}),
				schema(SchemaInstr::NextPlace {
					done: 6
				}),
				schema(SchemaInstr::Coerce {
					sql_type: true
				}),
				schema(SchemaInstr::Land),
				Instr::Jump {
					target: 2
				},
				schema(SchemaInstr::EndField),
				schema(SchemaInstr::Finish {
					computed: Arc::from(Vec::new())
				}),
				Instr::EndRow,
			]
		);
		assert!(program.host_code.is_empty());
		assert_eq!(
			emitted(&run, WriteStageKind::CleanupTableFields).expect("it lowers"),
			[
				schema(SchemaInstr::Cleanup {
					table: plan_of(&["a"])
				}),
				Instr::EndRow
			]
		);
	}

	#[test]
	fn the_clauses_of_a_field_are_host_pipelines_in_the_frame_the_host_binds_for_its_place() {
		let mut b = Builder::new();
		let stage = WriteStageKind::ProcessTableFields;
		let owner = field_stage_owner(&mut b, stage);
		let kinds = [FieldRole::Default, FieldRole::Value, FieldRole::Assert];
		let [default, value, assert] = kinds.map(|role| {
			// The `VALUE` clause reads the place's value; the others read nothing of it.
			let reads = role == FieldRole::Value;
			field_region(&mut b, owner, RegionKind::FieldFrame(role), EvalFrame::caller(), reads)
		});
		let field = FieldClauses {
			default: Some(UNBOUND),
			value: Some(UNBOUND),
			assert: Some(UNBOUND),
			..bare(0)
		};
		let regions = FieldRegions {
			default: vec![default],
			value: vec![value],
			assert: vec![assert],
			..FieldRegions::default()
		};
		let run = schema_run(vec![FieldStep::Field(field)], &["a"], &[], regions);
		let (code, program) = emitted_program(&b.ir, &run, stage).expect("it lowers");
		assert_eq!(
			code,
			[
				schema(SchemaInstr::Begin {
					table: plan_of(&["a"]),
					end: 14
				}),
				schema(SchemaInstr::Field {
					field: 0,
					write: permits_all()
				}),
				schema(SchemaInstr::NextPlace {
					done: 11
				}),
				schema(SchemaInstr::Default {
					pipeline: 0
				}),
				// A place holding nothing is produced by the `VALUE` and coerced after it.
				schema(SchemaInstr::JumpIfPlaceNone {
					target: 6
				}),
				schema(SchemaInstr::Coerce {
					sql_type: false
				}),
				schema(SchemaInstr::Value {
					pipeline: 1
				}),
				schema(SchemaInstr::Coerce {
					sql_type: true
				}),
				schema(SchemaInstr::DeferAssert),
				schema(SchemaInstr::Land),
				Instr::Jump {
					target: 2
				},
				schema(SchemaInstr::EndField),
				schema(SchemaInstr::Asserts {
					pipelines: Arc::from(vec![Some(2)])
				}),
				schema(SchemaInstr::Finish {
					computed: Arc::from(Vec::new())
				}),
				Instr::EndRow,
			]
		);
		// No program frame is named: the host binds the field's frame and the place's parameters.
		assert!(program.permissions.is_empty());
		assert_eq!(program.host_code.len(), 3);
		for pipeline in &program.host_code {
			assert!(matches!(pipeline.source, Source::Once));
			assert!(matches!(
				pipeline.code[pipeline.code.len() - 2..],
				[Instr::Emit { .. }, Instr::EndRow]
			));
		}
		// `$value` is read as the parameter the host binds, and only the names a clause reads are.
		let reads = |at: usize| {
			program.host_code[at]
				.code
				.iter()
				.filter(|instr| {
					matches!(
						instr,
						Instr::LoadParam {
							frame: None,
							..
						}
					)
				})
				.count()
		};
		assert_eq!((reads(0), reads(1), reads(2)), (0, 1, 0));
		assert_eq!(program.params.len(), 1);
		assert_eq!(program.params[0].name.as_str(), "value");
	}

	#[test]
	fn a_field_permission_is_judged_with_the_computed_fields_it_reads_derived_first() {
		let mut b = Builder::new();
		let stage = WriteStageKind::ProcessTableFields;
		let owner = field_stage_owner(&mut b, stage);
		let predicate = field_region(
			&mut b,
			owner,
			RegionKind::PermissionFrame,
			EvalFrame::mutable_permission(None),
			false,
		);
		let derivation = field_region(
			&mut b,
			owner,
			RegionKind::FieldFrame(FieldRole::Computed),
			EvalFrame::permission(None),
			false,
		);
		let field = FieldClauses {
			create: FieldGate::Predicate {
				caller_bound: false,
				derives: vec![derived_field("net")].into(),
			},
			update: FieldGate::Deny,
			..bare(0)
		};
		let regions = FieldRegions {
			permission: vec![predicate],
			predicate_derive: vec![derivation],
			..FieldRegions::default()
		};
		let run = schema_run(vec![FieldStep::Field(field)], &["a", "net"], &["net"], regions);
		let (code, program) = emitted_program(&b.ir, &run, stage).expect("it lowers");
		assert_eq!(
			code,
			[
				schema(SchemaInstr::Begin {
					table: plan_of(&["a", "net"]),
					end: 9
				}),
				schema(SchemaInstr::Field {
					field: 0,
					write: FieldWrite {
						create: FieldPermission::Predicate(0),
						update: FieldPermission::Deny
					}
				}),
				schema(SchemaInstr::NextPlace {
					done: 7
				}),
				schema(SchemaInstr::Coerce {
					sql_type: true
				}),
				// A field that does not permit every change keeps the value the record held.
				schema(SchemaInstr::Permit),
				schema(SchemaInstr::Land),
				Instr::Jump {
					target: 2
				},
				schema(SchemaInstr::EndField),
				schema(SchemaInstr::Finish {
					computed: Arc::from(vec![Idiom::field(Strand::new("net"))])
				}),
				Instr::EndRow,
			]
		);
		// The predicate runs in the host's frame, and the field it derives in the frame of the
		// permission: reads unchecked, writes refused.
		assert_eq!(
			program.permissions,
			vec![PermissionSpec {
				mode: FrameMode::Derived,
				auth: None,
				column: false,
			}]
		);
		let [verdict] = program.host_code.as_slice() else {
			panic!("the predicate is one host pipeline");
		};
		assert!(verdict.code.iter().any(|instr| matches!(instr, Instr::BeginComputing { .. })));
		assert_eq!(program.computed_names, vec![vec![Strand::new("net")]]);
		// A predicate states the computed fields it derives, and the stage hosts a region for each.
		let mut short = run;
		short.fields.predicate_derive.clear();
		assert!(matches!(emitted_in(&b.ir, &short, stage), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn a_computed_field_another_clause_reads_is_derived_among_every_computed_field() {
		let mut b = Builder::new();
		let stage = WriteStageKind::ProcessTableFields;
		let owner = field_stage_owner(&mut b, stage);
		let derivation = field_region(
			&mut b,
			owner,
			RegionKind::FieldFrame(FieldRole::Computed),
			EvalFrame::computed(None),
			false,
		);
		let steps = vec![FieldStep::Derive(derived_field("net")), FieldStep::Field(bare(0))];
		let regions = FieldRegions {
			derive: vec![derivation],
			..FieldRegions::default()
		};
		let run = schema_run(steps, &["a", "net", "total"], &["net", "total"], regions);
		let (code, program) = emitted_program(&b.ir, &run, stage).expect("it lowers");
		assert_eq!(
			code[1],
			schema(SchemaInstr::Derive {
				pipeline: 0,
				computed: Arc::from(vec![
					Idiom::field(Strand::new("net")),
					Idiom::field(Strand::new("total"))
				])
			})
		);
		assert_eq!(
			code[code.len() - 2],
			schema(SchemaInstr::Finish {
				computed: Arc::from(vec![
					Idiom::field(Strand::new("net")),
					Idiom::field(Strand::new("total"))
				])
			})
		);
		assert_eq!(
			program.permissions,
			vec![PermissionSpec {
				mode: FrameMode::Computed,
				auth: None,
				column: false,
			}]
		);
		// A self-dereference reads the image's stored fields unless it carries a key under the
		// name of a computed field, every one of which the table has, derived or not.
		assert_eq!(program.computed_names, vec![vec![Strand::new("net"), Strand::new("total")]]);
	}

	#[test]
	fn producing_clauses_that_read_each_other_refuse_every_record_but_an_import() {
		let b = Builder::new();
		let mut run = schema_run(Vec::new(), &["a", "b"], &[], FieldRegions::default());
		run.stored.schema.cycle = Some("a, b".into());
		assert_eq!(
			emitted(&run, WriteStageKind::ProcessTableFields).expect("it lowers"),
			[
				Instr::Write(WriteInstr::JumpIfImport {
					target: 2
				}),
				schema(SchemaInstr::Cycle {
					table: surrealdb_strand::TableName::new(Strand::new("person")),
					fields: Arc::from("a, b"),
				}),
				Instr::EndRow,
			]
		);
		drop(b);
	}

	#[test]
	fn a_schema_that_states_no_form_or_a_clause_no_region_holds_is_an_invariant_break() {
		let b = Builder::new();
		let stage = WriteStageKind::ProcessTableFields;
		let mut run =
			schema_run(vec![FieldStep::Field(bare(0))], &["a"], &[], FieldRegions::default());
		run.stored.schema.unlowerable = true;
		assert!(matches!(emitted_in(&b.ir, &run, stage), Err(IrError::Invariant { .. })));
		// A table that declares fields and states none of them.
		let run = schema_run(Vec::new(), &[], &[], FieldRegions::default());
		assert!(matches!(emitted_in(&b.ir, &run, stage), Err(IrError::Invariant { .. })));
		assert!(matches!(
			emitted_in(&b.ir, &run, WriteStageKind::CleanupTableFields),
			Err(IrError::Invariant { .. })
		));
		// A clause the schema states and the stage does not host.
		let field = FieldClauses {
			value: Some(UNBOUND),
			..bare(0)
		};
		let run = schema_run(vec![FieldStep::Field(field)], &["a"], &[], FieldRegions::default());
		assert!(matches!(emitted_in(&b.ir, &run, stage), Err(IrError::Invariant { .. })));
	}

	/// A clause of a field under an `AUTH LIMIT` that narrows the caller lowers when it holds a
	/// statement or a call, which run as units planned for the narrowed principal, and answers
	/// `NotYet` when it makes a closure, whose body no unit of that principal is requested for.
	#[test]
	fn a_clause_of_a_field_under_an_auth_limit_that_makes_a_closure_is_not_yet() {
		let stage = WriteStageKind::ProcessTableFields;
		let limit = surrealdb_iam::AuthLimit::new(surrealdb_iam::Level::Root, None);
		let limited = || EvalFrame {
			auth: Some(limit.clone()),
			..EvalFrame::caller()
		};
		for (frame, caller_bound, closure, lowers) in [
			(EvalFrame::caller(), true, false, true),
			(limited(), false, false, true),
			(limited(), true, false, true),
			(limited(), true, true, false),
		] {
			let mut b = Builder::new();
			let owner = field_stage_owner(&mut b, stage);
			let region = match closure {
				false => field_region(
					&mut b,
					owner,
					RegionKind::FieldFrame(FieldRole::Value),
					frame,
					false,
				),
				true => {
					let id = b.frame(frame);
					let args = vec![TypeKnowledge::Unknown; 5];
					b.in_frame(id, |b| {
						b.region_with_args(
							owner,
							RegionKind::FieldFrame(FieldRole::Value),
							&args,
							|b, _| b.closure(),
						);
					});
					*b.ir.op(owner).expect("the stage op").regions.last().expect("a region")
				}
			};
			let field = FieldClauses {
				value: Some(FieldClause {
					caller_bound,
				}),
				..bare(0)
			};
			let regions = FieldRegions {
				value: vec![region],
				..FieldRegions::default()
			};
			let run = schema_run(vec![FieldStep::Field(field)], &["a"], &[], regions);
			let lowered = emitted_in(&b.ir, &run, stage);
			match lowers {
				true => assert!(lowered.is_ok(), "{caller_bound} {closure}: {:?}", lowered.err()),
				false => assert!(matches!(lowered, Err(IrError::NotYet { .. }))),
			}
		}
	}

	#[test]
	fn the_default_of_the_id_field_is_a_host_pipeline_of_the_stage_that_generates_the_id() {
		let mut b = Builder::new();
		let stage = WriteStageKind::GenerateRecordId;
		let owner = field_stage_owner(&mut b, stage);
		let region = field_region(
			&mut b,
			owner,
			RegionKind::FieldFrame(FieldRole::Default),
			EvalFrame::caller(),
			false,
		);
		let run = StageRun {
			image: WriteImage {
				view: ImageView::Plain,
				keyed: false,
				id_default: true,
			},
			id_default: Some(region),
			..run()
		};
		let (code, program) = emitted_program(&b.ir, &run, stage).expect("it lowers");
		assert_eq!(
			code,
			[
				data(DataInstr::GenerateId {
					default: Some(0)
				}),
				Instr::EndRow
			]
		);
		assert_eq!(program.host_code.len(), 1);
		// An image that states a `DEFAULT` the stage does not host, and a region the image does not
		// state, are broken invariants and never a generated id.
		let missing = StageRun {
			id_default: None,
			..run.clone()
		};
		assert!(matches!(emitted_in(&b.ir, &missing, stage), Err(IrError::Invariant { .. })));
		let unstated = StageRun {
			image: WriteImage::default(),
			..run
		};
		assert!(matches!(
			emitted_in(&b.ir, &unstated, stage),
			Err(IrError::NotYet { .. } | IrError::Invariant { .. })
		));
		// With no `DEFAULT` the id is the one the table synthesises.
		assert_eq!(
			code_of_plain_id(),
			[
				data(DataInstr::GenerateId {
					default: None
				}),
				Instr::EndRow
			]
		);
	}

	fn code_of_plain_id() -> Vec<Instr> {
		code(WriteStageKind::GenerateRecordId)
	}

	// ----- The references and the indexes -------------------------------------------------------

	fn idiom(text: &str) -> Idiom {
		crate::syn::idiom(text).expect("an idiom")
	}

	fn index_def(
		id: u32,
		name: &str,
		cols: &[&str],
		index: crate::catalog::Index,
	) -> crate::catalog::IndexDefinition {
		crate::catalog::IndexDefinition {
			index_id: crate::catalog::IndexId(id),
			name: Strand::new(name),
			table_name: surrealdb_strand::TableName::new(Strand::new("person")),
			cols: cols.iter().map(|col| idiom(col)).collect(),
			index,
			count_cond: None,
			prepare_remove: false,
			format_version: 3,
			comment: None,
			nulls_not_distinct: false,
		}
	}

	/// A `COUNT` index that counts the records whose `active` is true.
	fn guarded_count(id: u32) -> crate::catalog::IndexDefinition {
		crate::catalog::IndexDefinition {
			count_cond: Some(crate::expr::Cond(
				crate::syn::expr("active = true").expect("an expression"),
			)),
			..index_def(id, "live", &[], crate::catalog::Index::Count(None))
		}
	}

	/// A stage op of `StoreIndexData` hosting one `Clause(IndexColumn)` region under `frame` for
	/// each of the fields `fields`, which reads the field off the image.
	fn column_regions(b: &mut Builder, frame: EvalFrame, fields: &[&str]) -> Vec<RegionId> {
		let owner = field_stage_owner(b, WriteStageKind::StoreIndexData);
		let id = b.frame(frame);
		b.in_frame(id, |b| {
			for field in fields {
				b.region_with_args(
					owner,
					RegionKind::Clause(ClauseRole::IndexColumn),
					&[TypeKnowledge::Unknown],
					|b, args| {
						b.add(
							ExprOp::Access {
								field: Strand::new(*field),
							},
							&[args[0]],
						)
					},
				);
			}
		});
		b.ir.op(owner).expect("the stage op").regions.to_vec()
	}

	fn indexing(defs: Vec<crate::catalog::IndexDefinition>, regions: Vec<RegionId>) -> StageRun {
		StageRun {
			stored: WriteStored {
				index_set: WriteIndexes {
					defs: IndexDefs::new(defs.into()),
					unlowerable: false,
				},
				..storing(Stored::Declared)
			},
			indexes: regions,
			..run()
		}
	}

	fn jump_if_fact(fact: StorageFact, target: Label) -> Instr {
		storage(StorageInstr::JumpIf {
			fact,
			when: true,
			target,
		})
	}

	#[test]
	fn the_reference_fields_of_a_table_are_synced_on_a_write_and_released_on_a_delete() {
		use crate::exec::ir::logical::stages::stage_run;
		use crate::exec::operators::write::plan::{WriteFacts, WriteForm as PlanForm};
		let fields: Box<[Idiom]> = Box::new([idiom("owner"), idiom("team.lead")]);
		let sync = |release| {
			storage(StorageInstr::SyncReferences {
				fields: fields.iter().cloned().collect(),
				release,
			})
		};
		// Over each form's own run, a writing form syncs the keys of every `REFERENCE` field and a
		// delete releases them. A create or an update stores nothing for a record of a `DROP`
		// table, so it syncs no key for the image it discards. A delete's purge removes the stored
		// record whatever the clause says, so it releases the record's keys. A view row's
		// maintenance keeps no reference key.
		for (form, expected) in [
			(PlanForm::Create, Some(false)),
			(PlanForm::Update, Some(false)),
			(PlanForm::Upsert, Some(false)),
			(PlanForm::Insert, Some(false)),
			(PlanForm::Relate, Some(false)),
			(PlanForm::Delete, Some(true)),
			(PlanForm::View, None),
		] {
			let stages = stage_run(
				form,
				WriteFacts {
					form,
					has_data: true,
					on_duplicate_key_update: false,
					ignore: false,
					repeatable: false,
				},
			);
			let held: Vec<_> = stages
				.iter()
				.filter(|stage| {
					matches!(
						stage,
						WriteStageKind::ProcessTableReferences
							| WriteStageKind::CleanupTableReferences
					)
				})
				.copied()
				.collect();
			let run = StageRun {
				stages,
				..stored_run(WriteStored {
					reference_fields: fields.clone(),
					materialized: Box::default(),
					..storing(Stored::Declared)
				})
			};
			let Some(release) = expected else {
				assert!(held.is_empty(), "{form:?} holds {held:?}");
				continue;
			};
			let [stage] = held[..] else {
				panic!("{form:?} holds {held:?}");
			};
			let code = emitted(&run, stage).expect("it lowers");
			if release {
				assert_eq!(code, [sync(true), Instr::EndRow], "{form:?}");
			} else {
				assert_eq!(
					code,
					[jump_if_fact(StorageFact::TableDropped, 2), sync(false), Instr::EndRow],
					"{form:?}"
				);
			}
		}
	}

	#[test]
	fn a_table_that_declares_references_names_at_least_one() {
		let run = stored_run(storing(Stored::Declared));
		for stage in
			[WriteStageKind::ProcessTableReferences, WriteStageKind::CleanupTableReferences]
		{
			assert!(
				matches!(emitted(&run, stage), Err(IrError::Invariant { .. })),
				"{stage:?} of a table that declares references and names none"
			);
		}
	}

	#[test]
	fn a_btree_index_is_maintained_from_the_values_its_columns_answer_for_both_images() {
		let mut b = Builder::new();
		let regions = column_regions(&mut b, EvalFrame::caller(), &["name", "age"]);
		let def = index_def(1, "by_name", &["name", "age"], crate::catalog::Index::Idx);
		let run = indexing(vec![def.clone()], regions);
		let (code, program) =
			emitted_program(&b.ir, &run, WriteStageKind::StoreIndexData).expect("it lowers");
		let lowered = Arc::new(LoweredIndexes::new(
			Arc::from(vec![def]),
			vec![IndexPlan::Entries {
				columns: 0,
			}],
		));
		assert_eq!(
			code,
			[
				jump_if_fact(StorageFact::Unmaintained, 4),
				jump_if_fact(StorageFact::TableDropped, 4),
				storage(StorageInstr::CheckIndexes {
					lowered: Arc::clone(&lowered),
				}),
				storage(StorageInstr::IndexEntries {
					lowered,
					position: 0,
				}),
				Instr::EndRow,
			]
		);
		// One host pipeline answers both columns, as the array of their values, in the frame an
		// index evaluates in.
		let [columns] = program.host_code.as_slice() else {
			panic!("the columns are one host pipeline");
		};
		assert!(matches!(columns.source, Source::Once));
		assert!(matches!(
			columns.code.first(),
			Some(Instr::BeginComputing {
				doc: Some(_),
				..
			})
		));
		let pushed =
			columns.code.iter().filter(|instr| matches!(instr, Instr::ArrayPush { .. })).count();
		assert_eq!(pushed, 2);
		assert!(matches!(
			columns.code[columns.code.len() - 2..],
			[Instr::Emit { .. }, Instr::EndRow]
		));
		assert!(!program.permissions.is_empty());
		assert!(program.permissions.iter().all(|spec| *spec
			== PermissionSpec {
				mode: FrameMode::IndexEntry,
				auth: None,
				column: false,
			}));
	}

	#[test]
	fn an_index_that_keeps_its_own_structure_is_moved_through_it_with_its_guard() {
		let mut b = Builder::new();
		let regions = column_regions(&mut b, EvalFrame::caller(), &["active"]);
		let def = guarded_count(7);
		let run = indexing(vec![def.clone()], regions);
		let (code, program) =
			emitted_program(&b.ir, &run, WriteStageKind::StoreIndexData).expect("it lowers");
		let lowered = Arc::new(LoweredIndexes::new(
			Arc::from(vec![def]),
			vec![IndexPlan::Native {
				kind: NativeIndexKind::Count,
				columns: 0,
				guard: Some(1),
			}],
		));
		assert_eq!(
			code[3..],
			[
				storage(StorageInstr::IndexNative {
					lowered,
					position: 0,
				}),
				Instr::EndRow,
			]
		);
		// The columns of a `COUNT` index are none, and its guard is a pipeline of its own over an
		// image that need not exist.
		assert_eq!(program.host_code.len(), 2);
		let [columns, guard] = program.host_code.as_slice() else {
			panic!("a count index with a guard is two host pipelines");
		};
		assert!(!columns.code.iter().any(|instr| matches!(instr, Instr::ArrayPush { .. })));
		assert!(!guard.code.iter().any(|instr| matches!(instr, Instr::BeginComputing { .. })));
	}

	#[test]
	fn an_index_marked_for_removal_is_not_maintained_but_is_still_checked() {
		let removed = crate::catalog::IndexDefinition {
			prepare_remove: true,
			..index_def(2, "old", &["name"], crate::catalog::Index::Uniq)
		};
		let run = indexing(vec![removed.clone()], Vec::new());
		let (code, program) =
			emitted_program(&Builder::new().ir, &run, WriteStageKind::StoreIndexData)
				.expect("it lowers");
		assert_eq!(
			code,
			[
				jump_if_fact(StorageFact::Unmaintained, 3),
				jump_if_fact(StorageFact::TableDropped, 3),
				storage(StorageInstr::CheckIndexes {
					lowered: Arc::new(LoweredIndexes::new(
						Arc::from(vec![removed]),
						vec![IndexPlan::Removed]
					)),
				}),
				Instr::EndRow,
			]
		);
		assert!(program.host_code.is_empty());
	}

	#[test]
	fn the_indexes_of_a_drop_table_follow_every_write_that_changes_its_storage() {
		use crate::exec::ir::logical::stages::stage_run;
		use crate::exec::operators::write::plan::{WriteFacts, WriteForm as PlanForm};
		let skips = |code: &[Instr]| -> Vec<StorageFact> {
			code.iter()
				.filter_map(|instr| {
					let Instr::Write(WriteInstr::Storage(StorageInstr::JumpIf {
						fact,
						..
					})) = instr
					else {
						return None;
					};
					Some(*fact)
				})
				.collect()
		};
		let def = index_def(1, "by_name", &["name"], crate::catalog::Index::Idx);
		// A create or an update stores nothing for a record of a `DROP` table, so the record's
		// entries are left as they are. A delete's purge removes the stored record, and a view's
		// maintenance writes and removes its rows, whatever the clause says, so their entries
		// follow the record. A table with no index is checked for one on the same terms.
		for (form, dropped) in [
			(PlanForm::Create, true),
			(PlanForm::Update, true),
			(PlanForm::Upsert, true),
			(PlanForm::Insert, true),
			(PlanForm::Relate, true),
			(PlanForm::Delete, false),
			(PlanForm::View, false),
		] {
			let stages = stage_run(
				form,
				WriteFacts {
					form,
					has_data: true,
					on_duplicate_key_update: false,
					ignore: false,
					repeatable: false,
				},
			);
			let expected = match dropped {
				true => vec![StorageFact::Unmaintained, StorageFact::TableDropped],
				false => vec![StorageFact::Unmaintained],
			};
			let mut b = Builder::new();
			let regions = column_regions(&mut b, EvalFrame::caller(), &["name"]);
			let indexed = StageRun {
				stages: stages.clone(),
				..indexing(vec![def.clone()], regions)
			};
			let (code, _) = emitted_program(&b.ir, &indexed, WriteStageKind::StoreIndexData)
				.expect("it lowers");
			assert_eq!(skips(&code), expected, "{form:?} over a table with an index");
			let unindexed = StageRun {
				stages,
				..stored_run(WriteStored {
					catalog_fixed: false,
					..storing(Stored::Nothing)
				})
			};
			let code = emitted(&unindexed, WriteStageKind::StoreIndexData).expect("it lowers");
			assert_eq!(skips(&code), expected, "{form:?} over a table with no index");
		}
	}

	#[test]
	fn the_indexes_of_a_table_follow_the_catalog_order() {
		let mut b = Builder::new();
		let regions = column_regions(&mut b, EvalFrame::caller(), &["a", "b", "c"]);
		let first = index_def(1, "first", &["a"], crate::catalog::Index::Idx);
		let gone = crate::catalog::IndexDefinition {
			prepare_remove: true,
			..index_def(2, "gone", &["z"], crate::catalog::Index::Idx)
		};
		let second = index_def(3, "second", &["b", "c"], crate::catalog::Index::Uniq);
		let run = indexing(vec![first, gone, second], regions);
		let (code, program) =
			emitted_program(&b.ir, &run, WriteStageKind::StoreIndexData).expect("it lowers");
		let positions = code
			.iter()
			.filter_map(|instr| {
				let Instr::Write(WriteInstr::Storage(StorageInstr::IndexEntries {
					position,
					..
				})) = instr
				else {
					return None;
				};
				Some(*position)
			})
			.collect::<Vec<_>>();
		assert_eq!(positions, [0, 2]);
		assert_eq!(program.host_code.len(), 2);
	}

	#[test]
	fn a_column_the_indexes_state_but_the_stage_does_not_host_is_never_skipped() {
		let def = index_def(1, "by_name", &["name"], crate::catalog::Index::Idx);
		let run = indexing(vec![def], Vec::new());
		assert!(matches!(
			emitted(&run, WriteStageKind::StoreIndexData),
			Err(IrError::Invariant { .. })
		));
	}

	#[test]
	fn a_region_the_indexes_do_not_state_is_never_read_as_a_column() {
		let mut b = Builder::new();
		let regions = column_regions(&mut b, EvalFrame::caller(), &["name", "extra"]);
		let def = index_def(1, "by_name", &["name"], crate::catalog::Index::Idx);
		let run = indexing(vec![def], regions);
		assert!(matches!(
			emitted_in(&b.ir, &run, WriteStageKind::StoreIndexData),
			Err(IrError::Invariant { .. })
		));
	}

	#[test]
	fn an_index_with_a_column_no_operator_runs_is_a_broken_invariant() {
		let def = index_def(1, "by_name", &["name"], crate::catalog::Index::Idx);
		let mut run = indexing(vec![def], Vec::new());
		run.stored.index_set.unlowerable = true;
		assert!(matches!(
			emitted(&run, WriteStageKind::StoreIndexData),
			Err(IrError::Invariant { .. })
		));
	}

	#[test]
	fn an_index_column_is_evaluated_in_the_frame_of_the_statement() {
		let mut b = Builder::new();
		for frame in [EvalFrame::permission(None), EvalFrame::computed(None)] {
			let regions = column_regions(&mut b, frame, &["name"]);
			let def = index_def(1, "by_name", &["name"], crate::catalog::Index::Idx);
			assert!(
				matches!(
					emitted_in(
						&b.ir,
						&indexing(vec![def], regions),
						WriteStageKind::StoreIndexData
					),
					Err(IrError::Invariant { .. })
				),
				"a column under another frame"
			);
		}
	}

	#[test]
	fn an_index_column_of_a_write_under_another_frame_is_evaluated_in_that_frame() {
		let mut b = Builder::new();
		let id = b.frame(EvalFrame::mutable_permission(None));
		let regions =
			b.in_frame(id, |b| column_regions(b, EvalFrame::mutable_permission(None), &["name"]));
		let def = index_def(1, "by_name", &["name"], crate::catalog::Index::Uniq);
		emitted_in(&b.ir, &indexing(vec![def], regions), WriteStageKind::StoreIndexData)
			.expect("a column in the frame of its stage lowers");
	}

	#[test]
	fn a_table_with_no_index_still_maintains_nothing_for_a_statement_that_cannot_define_one() {
		let run = stored_run(storing(Stored::Nothing));
		assert_eq!(
			emitted(&run, WriteStageKind::StoreIndexData).expect("it lowers"),
			[Instr::EndRow]
		);
	}
	// ----- The views ----------------------------------------------------------------------------

	/// A stage op of `ProcessTableViews` hosting one `Clause(ViewClause)` region under `frame` for
	/// each entry of `reads`: the field it reads off the image, or the image itself for `None`.
	fn view_regions(b: &mut Builder, frame: EvalFrame, reads: &[Option<&str>]) -> Vec<RegionId> {
		let owner = field_stage_owner(b, WriteStageKind::ProcessTableViews(Action::Update));
		let id = b.frame(frame);
		b.in_frame(id, |b| {
			for read in reads {
				b.region_with_args(
					owner,
					RegionKind::Clause(ClauseRole::ViewClause),
					&[TypeKnowledge::Unknown],
					|b, args| match read {
						Some(field) => b.add(
							ExprOp::Access {
								field: Strand::new(*field),
							},
							&[args[0]],
						),
						None => args[0],
					},
				);
			}
		});
		b.ir.op(owner).expect("the stage op").regions.to_vec()
	}

	/// The frame of a view's clauses.
	fn view_frame() -> EvalFrame {
		EvalFrame::mutable_permission(None)
	}

	fn viewing(defs: Vec<crate::catalog::TableDefinition>, regions: Vec<RegionId>) -> StageRun {
		StageRun {
			stored: WriteStored {
				view_set: WriteViews {
					defs: TableDefs::new(defs.into()),
					unlowerable: false,
				},
				..storing(Stored::Declared)
			},
			views: regions,
			..run()
		}
	}

	/// How many regions the views `defs` state.
	fn view_region_count(defs: &[crate::catalog::TableDefinition]) -> usize {
		crate::exec::ir::logical::stages::views_over(&Arc::from(defs.to_vec())).regions.len()
	}

	/// The code the stages `stages` emit under `run` over the container `ir`, in a lowering
	/// context, the program that holds their host pipelines and the units they asked for.
	fn emitted_views(
		ir: &Ir,
		run: &StageRun,
		stages: &[WriteStageKind],
	) -> Result<
		(Vec<Instr>, crate::exec::vm::program::Program, Vec<super::super::units::Needed>),
		IrError,
	> {
		let mut em = Emitter::new();
		let skip = em.label()?;
		let mut units = LowerCx::detached();
		{
			let mut cx = Stages::new(
				run,
				ir,
				LowerLimits::default(),
				crate::exec::ir::analysis::cx::builtin_registry(),
			)
			.in_context(&mut units);
			for stage in stages {
				emit_stage(&mut em, &mut cx, *stage, skip)?;
			}
		}
		em.bind(skip)?;
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once)?;
		let code = pipeline.code.clone();
		let program = em.finish_program(vec![pipeline], crate::exec::AccessMode::ReadOnly)?;
		Ok((code, program, units.take_needed()))
	}

	/// The views an instruction of `ProcessTableViews` maintains.
	fn maintained(instr: &Instr) -> (Action, &Arc<LoweredViews>) {
		let Instr::Write(WriteInstr::Observe(ObserveInstr::Views {
			action,
			lowered,
		})) = instr
		else {
			panic!("a views instruction: {instr:?}");
		};
		(*action, lowered)
	}

	#[test]
	fn a_materialized_view_is_maintained_from_its_condition_and_its_projection() {
		let mut b = Builder::new();
		let regions = view_regions(&mut b, view_frame(), &[Some("a"), None]);
		let defs = vec![view_table_for_test("m", "a, b AS c", Some("a > 1"), None)];
		let run = viewing(defs, regions);
		let stage = WriteStageKind::ProcessTableViews(Action::Update);
		let (code, program, needed) = emitted_views(&b.ir, &run, &[stage]).expect("it lowers");
		assert_eq!(code.len(), 2);
		assert_eq!(code[1], Instr::EndRow);
		let (action, lowered) = maintained(&code[0]);
		assert_eq!(action, Action::Update);
		assert_eq!(lowered.definitions.len(), 1);
		let [
			LoweredView::Materialized {
				condition: Some(condition),
				projection,
				keyed: false,
				rows,
				reads_data: false,
			},
		] = lowered.views.as_slice()
		else {
			panic!("a materialized view with a condition: {:?}", lowered.views);
		};
		// The condition is a pipeline for an image that is an object and one for any value; the
		// projection is one pipeline for both.
		assert_ne!(condition.object, condition.any);
		assert_eq!(projection.object, projection.any);
		assert_eq!(program.host_code.len(), 3);
		// The order of the view table's rows is a unit of the program.
		let [
			super::super::units::Needed {
				request: UnitRequest::ViewRows {
					table,
				},
				id: Some(unit),
			},
		] = needed.as_slice()
		else {
			panic!("the order of the view's rows is the one unit asked for: {needed:?}");
		};
		assert_eq!(table.as_str(), "m");
		assert_eq!(*unit, rows.unit);
	}

	#[test]
	fn a_materialized_view_with_no_condition_keeps_every_record_the_write_did_not_delete() {
		let mut b = Builder::new();
		let regions = view_regions(&mut b, view_frame(), &[None]);
		let run = viewing(vec![view_table_for_test("p", "*", None, None)], regions);
		let stage = WriteStageKind::ProcessTableViews(Action::Create);
		let (code, program, _) = emitted_views(&b.ir, &run, &[stage]).expect("it lowers");
		let (_, lowered) = maintained(&code[0]);
		let [
			LoweredView::Materialized {
				condition: None,
				..
			},
		] = lowered.views.as_slice()
		else {
			panic!("a materialized view with no condition: {:?}", lowered.views);
		};
		// The projection of the image itself is one pipeline that emits the row.
		let [projection] = program.host_code.as_slice() else {
			panic!("the projection is one host pipeline");
		};
		assert!(matches!(
			projection.code.as_slice(),
			[
				Instr::Emit {
					src: ROW
				},
				Instr::EndRow
			]
		));
	}

	#[test]
	fn a_materialized_view_is_keyed_by_its_rows_id_only_where_its_projection_names_one() {
		for (fields, keyed) in [
			("*", false),
			("a, id", false),
			("a, a AS id", true),
			("*, [a] AS id.k", true),
			("a AS ids", false),
			("a, id.k", true),
			("a, id[0]", true),
		] {
			let mut b = Builder::new();
			let regions = view_regions(&mut b, view_frame(), &[None]);
			let run = viewing(vec![view_table_for_test("k", fields, None, None)], regions);
			let stage = WriteStageKind::ProcessTableViews(Action::Create);
			let (code, _, _) = emitted_views(&b.ir, &run, &[stage]).expect("it lowers");
			let (_, lowered) = maintained(&code[0]);
			let [
				LoweredView::Materialized {
					keyed: lowered_keyed,
					..
				},
			] = lowered.views.as_slice()
			else {
				panic!("a materialized view: {:?}", lowered.views);
			};
			assert_eq!(*lowered_keyed, keyed, "fields `{fields}`");
		}
	}

	#[test]
	fn an_aggregated_view_is_maintained_from_its_groups_its_arguments_and_its_fields() {
		let mut b = Builder::new();
		let defs = vec![view_table_for_test(
			"g",
			"a, count() AS n, math::max(x) AS top",
			Some("x != NONE"),
			Some(&["a"]),
		)];
		let count = view_region_count(&defs);
		let regions = view_regions(&mut b, view_frame(), &vec![Some("a"); count]);
		let Some(crate::catalog::ViewDefinition::Aggregated {
			analysis,
			..
		}) = defs[0].view.clone()
		else {
			panic!("the view is aggregated");
		};
		let run = viewing(defs, regions);
		let stage = WriteStageKind::ProcessTableViews(Action::Delete);
		let (code, program, needed) = emitted_views(&b.ir, &run, &[stage]).expect("it lowers");
		let (action, lowered) = maintained(&code[0]);
		assert_eq!(action, Action::Delete);
		let [
			LoweredView::Aggregated {
				condition: Some(_),
				group,
				arguments,
				fields,
				recalc: Some(recalc),
				rows,
				reads_data: false,
			},
		] = lowered.views.as_slice()
		else {
			panic!("an aggregated view with a condition and a recalculation: {:?}", lowered.views);
		};
		assert_eq!(group.len(), analysis.group_expressions.len());
		assert_eq!(arguments.len(), analysis.aggregate_arguments.len());
		let names: Vec<_> = fields
			.iter()
			.map(|(place, _)| match place.as_slice() {
				[PlaceStepSpec::Field(name)] => name.clone(),
				other => panic!("a field lands at one named step: {other:?}"),
			})
			.collect();
		assert_eq!(names, ["a", "n", "top"]);
		assert_eq!(recalc.extremes, extremes_of(&analysis));
		assert!(!recalc.extremes.is_empty());
		// Each clause is a pipeline for an image that is an object and one for any value.
		assert_eq!(program.host_code.len(), 2 * count);
		// The order of the view's rows, then the recalculation of its extremes.
		let requests: Vec<_> = needed.iter().map(|need| (&need.request, need.id)).collect();
		assert_eq!(
			requests,
			[
				(
					&UnitRequest::ViewRows {
						table: surrealdb_strand::TableName::new(Strand::new("g"))
					},
					Some(rows.unit)
				),
				(
					&UnitRequest::ViewRecalc {
						view: surrealdb_strand::TableName::new(Strand::new("g"))
					},
					Some(recalc.unit)
				),
			]
		);
	}

	#[test]
	fn a_view_that_keeps_no_extreme_asks_for_no_recalculation() {
		let mut b = Builder::new();
		let defs = vec![view_table_for_test("g", "a, count() AS n", None, Some(&["a"]))];
		let regions =
			view_regions(&mut b, view_frame(), &vec![Some("a"); view_region_count(&defs)]);
		let run = viewing(defs, regions);
		let stage = WriteStageKind::ProcessTableViews(Action::Update);
		let (code, _, needed) = emitted_views(&b.ir, &run, &[stage]).expect("it lowers");
		let (_, lowered) = maintained(&code[0]);
		let [
			LoweredView::Aggregated {
				recalc: None,
				..
			},
		] = lowered.views.as_slice()
		else {
			panic!("an aggregated view that keeps no extreme: {:?}", lowered.views);
		};
		assert!(needed.iter().all(|need| matches!(need.request, UnitRequest::ViewRows { .. })));
	}

	#[test]
	fn a_subscript_a_field_name_embeds_is_a_host_pipeline_of_its_step() {
		let mut b = Builder::new();
		let defs = vec![view_table_for_test(
			"g",
			"tags[0] AS first[0], count() AS n",
			None,
			Some(&["tags[0]"]),
		)];
		let count = view_region_count(&defs);
		let regions = view_regions(&mut b, view_frame(), &vec![Some("a"); count]);
		let run = viewing(defs, regions);
		let stage = WriteStageKind::ProcessTableViews(Action::Update);
		let (code, program, _) = emitted_views(&b.ir, &run, &[stage]).expect("it lowers");
		let (_, lowered) = maintained(&code[0]);
		let [
			LoweredView::Aggregated {
				fields,
				..
			},
		] = lowered.views.as_slice()
		else {
			panic!("an aggregated view: {:?}", lowered.views);
		};
		let [
			PlaceStepSpec::Field(name),
			PlaceStepSpec::Subscript {
				expr,
				..
			},
		] = fields[0].0.as_slice()
		else {
			panic!("the first field lands at a name and a subscript: {:?}", fields[0].0);
		};
		assert_eq!(name, "first");
		// The subscript runs once with no record in scope: one pipeline that emits its value.
		let subscript = &program.host_code[usize::from(*expr)];
		assert!(matches!(subscript.source, Source::Once));
		assert!(matches!(
			subscript.code[subscript.code.len() - 2..],
			[Instr::Emit { .. }, Instr::EndRow]
		));
	}

	#[test]
	fn a_view_that_is_computed_when_read_is_not_maintained() {
		let mut computed = view_table_for_test("s", "a", None, None);
		let Some(crate::catalog::ViewDefinition::Materialized {
			fields,
			tables,
			..
		}) = computed.view.clone()
		else {
			panic!("the view is materialized");
		};
		computed.view = Some(crate::catalog::ViewDefinition::Select {
			fields,
			tables,
			condition: None,
			groups: None,
		});
		let run = viewing(vec![computed], Vec::new());
		let stage = WriteStageKind::ProcessTableViews(Action::Update);
		let (code, program, needed) =
			emitted_views(&Builder::new().ir, &run, &[stage]).expect("it lowers");
		let (_, lowered) = maintained(&code[0]);
		assert_eq!(lowered.views, [LoweredView::Unmaintained]);
		assert!(program.host_code.is_empty() && needed.is_empty());
	}

	#[test]
	fn the_views_of_a_write_are_lowered_once_for_every_stage_that_maintains_them() {
		let mut b = Builder::new();
		let regions = view_regions(&mut b, view_frame(), &[Some("a"), None]);
		let defs = vec![view_table_for_test("m", "a, b AS c", Some("a > 1"), None)];
		let run = viewing(defs, regions);
		let stages = [
			WriteStageKind::ProcessTableViews(Action::Create),
			WriteStageKind::ProcessTableViews(Action::Update),
		];
		let (code, program, needed) = emitted_views(&b.ir, &run, &stages).expect("it lowers");
		let (first, second) = (maintained(&code[0]), maintained(&code[1]));
		assert_eq!((first.0, second.0), (Action::Create, Action::Update));
		assert!(Arc::ptr_eq(first.1, second.1));
		assert_eq!(program.host_code.len(), 3);
		assert_eq!(needed.len(), 1);
	}

	#[test]
	fn a_view_clause_the_views_state_but_the_stage_does_not_host_is_never_skipped() {
		let mut b = Builder::new();
		let defs = vec![view_table_for_test("m", "a", Some("a > 1"), None)];
		let regions = view_regions(&mut b, view_frame(), &[Some("a")]);
		let run = viewing(defs, regions);
		let stage = WriteStageKind::ProcessTableViews(Action::Update);
		assert!(matches!(emitted_views(&b.ir, &run, &[stage]), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn a_region_the_views_do_not_state_is_never_read_as_a_clause() {
		let mut b = Builder::new();
		let defs = vec![view_table_for_test("m", "a", None, None)];
		let regions = view_regions(&mut b, view_frame(), &[None, Some("extra")]);
		let run = viewing(defs, regions);
		let stage = WriteStageKind::ProcessTableViews(Action::Update);
		assert!(matches!(emitted_views(&b.ir, &run, &[stage]), Err(IrError::Invariant { .. })));
	}

	#[test]
	fn a_view_with_a_clause_no_operator_runs_is_a_broken_invariant() {
		let mut run = viewing(vec![view_table_for_test("m", "a", None, None)], Vec::new());
		run.stored.view_set.unlowerable = true;
		let stage = WriteStageKind::ProcessTableViews(Action::Update);
		assert!(matches!(
			emitted_views(&Builder::new().ir, &run, &[stage]),
			Err(IrError::Invariant { .. })
		));
	}

	#[test]
	fn a_view_clause_is_evaluated_in_the_frame_of_a_view() {
		let mut b = Builder::new();
		for frame in [EvalFrame::caller(), EvalFrame::permission(None), EvalFrame::computed(None)] {
			let regions = view_regions(&mut b, frame, &[None]);
			let run = viewing(vec![view_table_for_test("m", "a", None, None)], regions);
			let stage = WriteStageKind::ProcessTableViews(Action::Update);
			assert!(
				matches!(emitted_views(&b.ir, &run, &[stage]), Err(IrError::Invariant { .. })),
				"a clause under another frame"
			);
		}
	}

	#[test]
	fn the_views_of_a_write_lowered_with_no_unit_table_are_a_broken_invariant() {
		let mut b = Builder::new();
		let regions = view_regions(&mut b, view_frame(), &[None]);
		let run = viewing(vec![view_table_for_test("m", "a", None, None)], regions);
		let stage = WriteStageKind::ProcessTableViews(Action::Update);
		assert!(matches!(emitted_in(&b.ir, &run, stage), Err(IrError::Invariant { .. })));
	}

	/// One repair of a field named `name`.
	fn repair(name: &str, strategy: RepairKind) -> RepairField {
		RepairField {
			name: Idiom::field(Strand::new(name)),
			strategy,
		}
	}

	/// The table `name` with the repairs `fields`.
	fn repaired(name: &str, fields: Vec<RepairField>) -> RepairTable {
		RepairTable {
			table: surrealdb_strand::TableName::new(Strand::new(name)),
			fields: fields.into_boxed_slice(),
		}
	}

	/// A run whose write states that the repairs `tables` follow a purge of its record, with
	/// `regions` as the `THEN` regions its purge stage hosts.
	fn referenced(tables: Vec<RepairTable>, regions: Vec<RegionId>) -> StageRun {
		StageRun {
			stored: WriteStored {
				referenced: Stored::Declared,
				repair_set: WriteRepairs {
					tables: tables.into_boxed_slice(),
					unlowerable: false,
				},
				..storing(Stored::Nothing)
			},
			repairs: regions,
			..run()
		}
	}

	/// A run whose write states that no field references its table.
	fn unreferenced() -> StageRun {
		StageRun {
			stored: WriteStored {
				referenced: Stored::Nothing,
				..storing(Stored::Nothing)
			},
			..run()
		}
	}

	/// The code the purge stage emits under `run` over `ir`, in a lowering context whose frame
	/// suppresses permission enforcement where `suppressed`, with the program that holds its host
	/// pipelines and the units it asked for.
	fn purged_in(
		ir: &Ir,
		run: &StageRun,
		suppressed: bool,
	) -> Result<
		(Vec<Instr>, crate::exec::vm::program::Program, Vec<super::super::units::Needed>),
		IrError,
	> {
		let mut em = Emitter::new();
		let skip = em.label()?;
		let mut units = LowerCx::detached();
		let frame = crate::exec::vm::program::UnitFrame {
			perms_suppressed: suppressed,
			..crate::exec::vm::program::UnitFrame::unprincipled()
		};
		let reads_checked = frame.checked;
		units.rescope(frame, reads_checked, 0);
		{
			let mut cx = Stages::new(
				run,
				ir,
				LowerLimits::default(),
				crate::exec::ir::analysis::cx::builtin_registry(),
			)
			.in_context(&mut units);
			emit_stage(&mut em, &mut cx, WriteStageKind::PurgeRecordData, skip)?;
		}
		em.bind(skip)?;
		em.emit(Instr::EndRow);
		let pipeline = em.finish_pipeline(Source::Once)?;
		let code = pipeline.code.clone();
		let program = em.finish_program(vec![pipeline], crate::exec::AccessMode::ReadOnly)?;
		Ok((code, program, units.take_needed()))
	}

	fn purged(
		run: &StageRun,
	) -> Result<
		(Vec<Instr>, crate::exec::vm::program::Program, Vec<super::super::units::Needed>),
		IrError,
	> {
		purged_in(&Builder::new().ir, run, false)
	}

	/// The repairs the `Repair` instruction of `code` holds.
	fn repairs_of_code(code: &[Instr]) -> &Arc<[TableRepairs]> {
		code.iter()
			.find_map(|instr| {
				let Instr::Write(WriteInstr::Storage(StorageInstr::References(
					ReferenceInstr::Repair {
						tables,
					},
				))) = instr
				else {
					return None;
				};
				Some(tables)
			})
			.expect("a purge repairs the records that reference the deleted one")
	}

	fn cascade(
		kind: CascadeKind,
		suppressed: bool,
		caller: bool,
		id: u32,
	) -> super::super::units::Needed {
		super::super::units::Needed {
			request: UnitRequest::Cascade {
				kind,
				suppressed,
				caller,
			},
			id: Some(UnitId(id)),
		}
	}

	fn table_named(name: &str) -> surrealdb_strand::TableName {
		surrealdb_strand::TableName::new(Strand::new(name))
	}

	/// A record is purged whether or not a field references its table: the references it holds are
	/// drained at run time from the keys the database stores, so the cascade always runs, and a
	/// record no phase changed is left alone.
	#[test]
	fn a_purge_deletes_the_record_and_runs_the_cascades_whether_or_not_a_field_references_it() {
		let (code, _, needed) = purged(&unreferenced()).expect("it lowers");
		let references = |instr| storage(StorageInstr::References(instr));
		assert_eq!(
			code,
			[
				storage(StorageInstr::JumpIf {
					fact: StorageFact::Unmodified,
					when: true,
					target: 10,
				}),
				storage(StorageInstr::DeleteRecord),
				storage(StorageInstr::JumpIf {
					fact: StorageFact::WasEdge,
					when: false,
					target: 4,
				}),
				storage(StorageInstr::PurgePointers),
				references(ReferenceInstr::PurgeEdges {
					unit: Some(UnitId(0)),
				}),
				references(ReferenceInstr::Drain {
					target: 10,
				}),
				references(ReferenceInstr::Next {
					target: 9,
				}),
				references(ReferenceInstr::Repair {
					tables: Arc::from(Vec::new()),
				}),
				Instr::Jump {
					target: 6,
				},
				references(ReferenceInstr::Release),
				Instr::EndRow,
			]
		);
		assert_eq!(needed, [cascade(CascadeKind::Edges, false, false, 0)]);
	}

	/// Each reference field is repaired by the strategy its clause directs, under the name a
	/// reference key carries for it, and the statements a repair runs are units asked for in the
	/// frame each runs in: a delete of a referencing record suppresses enforcement for it and keeps
	/// the writer's own, and a read or an update of one suppresses both.
	#[test]
	fn each_reference_field_is_repaired_by_the_strategy_its_clause_directs() {
		let author = repaired(
			"author",
			vec![
				repair("a", RepairKind::Ignore),
				repair("b", RepairKind::Reject),
				repair("c", RepairKind::Cascade),
				repair("d", RepairKind::Unset),
			],
		);
		let (code, _, needed) = purged(&referenced(vec![author], Vec::new())).expect("it lowers");
		let [repairs] = repairs_of_code(&code).as_ref() else {
			panic!("one table is repaired");
		};
		assert_eq!(repairs.table, table_named("author"));
		assert_eq!(
			repairs.fields,
			vec![
				("a".to_owned(), RepairStrategy::Ignore),
				("b".to_owned(), RepairStrategy::Reject),
				(
					"c".to_owned(),
					RepairStrategy::Cascade {
						unit: UnitId(0)
					}
				),
				(
					"d".to_owned(),
					RepairStrategy::Unset {
						field: Idiom::field(Strand::new("d")),
						read: UnitId(1),
						unset: UnitId(2),
						subtract: UnitId(3),
					}
				),
			]
		);
		let table = || table_named("author");
		let field = || Idiom::field(Strand::new("d"));
		assert_eq!(
			needed,
			[
				cascade(
					CascadeKind::Delete {
						table: table()
					},
					true,
					false,
					0
				),
				cascade(
					CascadeKind::Read {
						table: table()
					},
					true,
					true,
					1
				),
				cascade(
					CascadeKind::Unset {
						table: table(),
						field: field()
					},
					true,
					true,
					2
				),
				cascade(
					CascadeKind::Subtract {
						table: table(),
						field: field()
					},
					true,
					true,
					3
				),
				cascade(CascadeKind::Edges, false, false, 4),
			]
		);
	}

	/// A write lowered for a frame that suppresses permission enforcement runs its cascades
	/// suppressed too: the edge cascade and the delete of a referencing record keep that state for
	/// whatever they trigger.
	#[test]
	fn the_cascades_of_a_suppressed_write_keep_the_state_of_its_frame() {
		let run =
			referenced(vec![repaired("author", vec![repair("c", RepairKind::Cascade)])], vec![]);
		let (_, _, needed) = purged_in(&Builder::new().ir, &run, true).expect("it lowers");
		assert_eq!(
			needed,
			[
				cascade(
					CascadeKind::Delete {
						table: table_named("author")
					},
					true,
					true,
					0
				),
				cascade(CascadeKind::Edges, true, true, 1),
			]
		);
	}

	/// Two fields that cascade to one table share the one unit of the delete, and a table that
	/// references itself asks for the unit of its own delete once: the unit table finds a request
	/// again, which is how a cascade that reaches itself closes.
	#[test]
	fn repairs_that_delete_the_same_table_share_one_unit() {
		let person = repaired(
			"person",
			vec![repair("friend", RepairKind::Cascade), repair("boss", RepairKind::Cascade)],
		);
		let (code, _, needed) = purged(&referenced(vec![person], Vec::new())).expect("it lowers");
		let [repairs] = repairs_of_code(&code).as_ref() else {
			panic!("one table is repaired");
		};
		let units: Vec<_> = repairs.fields.iter().map(|(_, strategy)| strategy.clone()).collect();
		assert_eq!(
			units,
			[
				RepairStrategy::Cascade {
					unit: UnitId(0)
				},
				RepairStrategy::Cascade {
					unit: UnitId(0)
				}
			]
		);
		assert_eq!(needed.len(), 2, "the delete of the table, and the edge cascade");
	}

	/// The regions of a stage op of `PurgeRecordData` under `frame`, one for each body of
	/// `reads`: the field it reads off `$reference`, or the referencing record itself for `None`.
	fn repair_regions(b: &mut Builder, frame: EvalFrame, reads: &[Option<&str>]) -> Vec<RegionId> {
		let owner = field_stage_owner(b, WriteStageKind::PurgeRecordData);
		let id = b.frame(frame);
		b.in_frame(id, |b| {
			for read in reads {
				b.region_with_args(
					owner,
					RegionKind::Clause(ClauseRole::ReferenceRepair),
					&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
					|b, args| match read {
						Some(field) => b.add(
							ExprOp::Access {
								field: Strand::new(*field),
							},
							&[args[1]],
						),
						None => args[0],
					},
				);
			}
		});
		b.ir.op(owner).expect("the stage op").regions.to_vec()
	}

	/// A `THEN` repair is one host pipeline of the program over the id of the referencing record,
	/// which reads `$reference` through the frame the run binds, and the repairs take the regions
	/// of the stage in the order the fields state them.
	#[test]
	fn a_then_repair_is_a_host_pipeline_over_the_referencing_record() {
		let mut b = Builder::new();
		let regions =
			repair_regions(&mut b, EvalFrame::mutable_permission(None), &[Some("name"), None]);
		let author = repaired(
			"author",
			vec![
				repair("a", RepairKind::Custom),
				repair("b", RepairKind::Ignore),
				repair("c", RepairKind::Custom),
			],
		);
		let (code, program, needed) =
			purged_in(&b.ir, &referenced(vec![author], regions), false).expect("it lowers");
		let [repairs] = repairs_of_code(&code).as_ref() else {
			panic!("one table is repaired");
		};
		let clauses: Vec<_> = repairs.fields.iter().map(|(_, strategy)| strategy.clone()).collect();
		assert_eq!(
			clauses,
			[
				RepairStrategy::Custom {
					clause: 0
				},
				RepairStrategy::Ignore,
				RepairStrategy::Custom {
					clause: 1
				}
			]
		);
		assert_eq!(program.host_code.len(), 2);
		assert!(
			program.host_code[0].code.iter().any(|instr| matches!(instr, Instr::LoadParam { .. })),
			"`$reference` is read through the frame the run binds"
		);
		assert!(
			!program.host_code[1].code.iter().any(|instr| matches!(instr, Instr::LoadParam { .. })),
			"a clause that never reads `$reference` binds no parameter"
		);
		assert_eq!(needed.len(), 1, "a THEN repair asks for no unit but the edge cascade");
	}

	#[test]
	fn a_then_repair_is_evaluated_in_the_frame_of_a_purge() {
		let mut b = Builder::new();
		for frame in [EvalFrame::caller(), EvalFrame::permission(None), EvalFrame::computed(None)] {
			let regions = repair_regions(&mut b, frame, &[None]);
			let run = referenced(
				vec![repaired("author", vec![repair("a", RepairKind::Custom)])],
				regions,
			);
			assert!(
				matches!(purged_in(&b.ir, &run, false), Err(IrError::Invariant { .. })),
				"a clause under another frame"
			);
		}
	}

	/// A repair the references state with no region to run, or a region no repair states, is a
	/// broken invariant, and so is a purge lowered with no unit table to ask the cascades of.
	#[test]
	fn repairs_and_their_regions_must_agree_and_a_purge_needs_a_unit_table() {
		let mut b = Builder::new();
		let regions = repair_regions(&mut b, EvalFrame::mutable_permission(None), &[None]);
		let custom = || repaired("author", vec![repair("a", RepairKind::Custom)]);
		assert!(matches!(
			purged_in(&b.ir, &referenced(vec![custom()], Vec::new()), false),
			Err(IrError::Invariant { .. })
		));
		assert!(matches!(
			purged_in(&b.ir, &referenced(Vec::new(), regions.clone()), false),
			Err(IrError::Stale(_))
		));
		assert!(matches!(
			emitted_in(
				&b.ir,
				&referenced(vec![custom()], regions),
				WriteStageKind::PurgeRecordData
			),
			Err(IrError::Invariant { .. })
		));
	}

	/// A write that names no one table answers `NotYet` from the stage that needs the one table, a
	/// write bound before a catalog change answers `LateBind`, and repairs that state no region
	/// because a clause has no form are an invariant break: a purge never reads a repair the
	/// references do not state.
	#[test]
	fn a_purge_whose_references_are_not_stated_is_not_lowered() {
		let with = |referenced| StageRun {
			stored: WriteStored {
				referenced,
				..storing(Stored::Nothing)
			},
			..run()
		};
		assert!(matches!(
			purged(&with(Stored::Dynamic)),
			Err(IrError::NotYet { what }) if what.starts_with("ir_lower::stage: ")
		));
		assert!(matches!(purged(&with(Stored::LateBound)), Err(IrError::LateBind { .. })));
		let unlowerable = StageRun {
			stored: WriteStored {
				referenced: Stored::Declared,
				repair_set: WriteRepairs {
					tables: vec![repaired("author", vec![repair("a", RepairKind::Custom)])]
						.into_boxed_slice(),
					unlowerable: true,
				},
				..storing(Stored::Nothing)
			},
			..run()
		};
		assert!(matches!(purged(&unlowerable), Err(IrError::Invariant { .. })));
	}
}
