//! What a logical op does and how it is shaped: its local effects and fallibility, and the
//! structural facts the verifier reads.
//!
//! The effect and failure answers are local: they do not fold the ops in the op's regions, and a
//! statement op answers for the statement's own work, never for the expressions inside its
//! regions. The transitive answer is computed by the analysis pass.
//!
//! Every answer is an exhaustive `match` over the variants of [`LogOp`] with no wildcard arm, so a
//! variant added to the dialect does not compile until each answer names it.
//!
//! The structural answers state the shape a container must have for the op:
//!
//! - `terminator` names the kind of a terminator with a null target; the target of a `Return`,
//!   `Break` or `Next` is derived from the nesting of regions and never stored.
//! - `region_kinds` answers the kinds of the regions the attributes fix. A block, `IF` or `FOR`
//!   answers its statement kind; the value kind is chosen by position, which the op does not see.
//!   An op whose region count no attribute fixes answers the minimal form, one region of each
//!   listed kind.
//! - `arity` answers the operand rule and the result count; for an op whose operand count depends
//!   on its attributes the rule is the lower bound those attributes fix.

#![deny(clippy::wildcard_enum_match_arm)]

use slotmap::Key;
use smallvec::{SmallVec, smallvec};

use super::ops::{
	ComputedReadsShape, FieldShape, FieldsShape, LetBinding, LogOp, LookupAttrs, LoopKind,
	LoopSink, SortShape, SourceRefShape, StepAttrs, TxnRegionMode, UseTarget,
};
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::effects::{Determinism, IrFootprint, IrResource, OpEffects, Rollback};
use crate::exec::ir::iface::fallibility::{ContextDep, Fallibility};
use crate::exec::ir::iface::ids::RegionId;
use crate::exec::ir::iface::region_kind::{
	ClauseRole, ErrorScope, FieldRole, RegionKind, RegionKinds, StatementCommit, StoredRole,
	TxnKind,
};
use crate::exec::ir::iface::statements::{
	AccessOp, AccessSubjectShape, AccessTypeShape, AlterApiClauseShape, AlterDefaultShape,
	AlterObject, ApiConfigShape, ConfigShape, DefaultShape, DefineObject, IndexColumn, IndexSpec,
	InfoSubject, LiveShape, MatchData, OnDeleteShape, PermissionShape, PermissionsShape,
	ReferenceShape, RegionSlot, RemoveObject,
};
use crate::exec::ir::op_kind::{Arity, IsaCaps, NativeLowering, OperandRule, Terminator};
use crate::expr::logical::EffectSet;
use crate::expr::statements::alter::AlterKind;

/// The reads, writes and rejection of a statement that writes records.
const WRITE_SET: EffectSet =
	EffectSet::WRITES_DATA.union(EffectSet::READS_DATA).union(EffectSet::MAY_REJECT);

/// The work every `DEFINE`, `REMOVE`, `ALTER` and `REBUILD` does.
const DDL_SET: EffectSet = EffectSet::CHANGES_CATALOG.union(EffectSet::READS_CATALOG);

/// The work an `ACCESS ... GRANT`, `REVOKE` or `PURGE` does: it reads its access method and writes
/// the method's grants, which are catalog entries.
const ACCESS_WRITE: EffectSet = EffectSet::CHANGES_CATALOG.union(EffectSet::READS_CATALOG);

/// The (slot, kind) pairs of the regions an op's attributes name.
type Slots = SmallVec<[(RegionSlot, RegionKind); 4]>;

impl LogOp {
	/// The op's own effects, excluding the ops of its regions.
	///
	/// `fallibility` is the op's [`LogOp::fallibility`], so the two answers cannot disagree. The
	/// footprint is the one [`footprint_of`] derives from the effect set; keyed footprints are
	/// computed by the analysis pass.
	pub(crate) fn local_effects(&self, cx: &EffectCx<'_>) -> OpEffects {
		let (set, rollback) = self.effect_class();
		OpEffects {
			set,
			fallibility: self.fallibility(cx),
			context_deps: self.context_deps(),
			footprint: footprint_of(set),
			rollback,
			determinism: determinism_of(set),
		}
	}

	/// The effect flags of the op and whether a rollback undoes them.
	fn effect_class(&self) -> (EffectSet, Rollback) {
		match self {
			// A statement planned when its block reaches it may do whatever a statement does.
			LogOp::Statement {
				served: Some(_),
				..
			} => (EffectSet::TOP, Rollback::Survives),
			LogOp::Loop {
				kind: LoopKind::GraphStep,
				..
			} => (EffectSet::READS_DATA, Rollback::NoEffects),
			LogOp::Request {
				..
			}
			| LogOp::Txn {
				..
			}
			| LogOp::Stmt {
				..
			}
			| LogOp::Statement {
				..
			}
			| LogOp::Values {
				..
			}
			| LogOp::Match {
				..
			}
			| LogOp::MatchRead {
				..
			}
			| LogOp::MatchOutput(..)
			| LogOp::Subquery
			| LogOp::UnionAll
			| LogOp::Join {
				..
			}
			| LogOp::Filter
			| LogOp::Split {
				..
			}
			| LogOp::Aggregate(..)
			| LogOp::Distinct
			| LogOp::Sort(..)
			| LogOp::Window(..)
			| LogOp::Limit {
				..
			}
			| LogOp::Project(..)
			| LogOp::Timeout
			| LogOp::VersionScope
			| LogOp::Only
			| LogOp::Explain {
				..
			}
			| LogOp::Eval
			| LogOp::Block
			| LogOp::Return
			| LogOp::Loop {
				..
			}
			| LogOp::If
			| LogOp::Choose(..)
			| LogOp::Yield
			| LogOp::Next
			| LogOp::Break => (EffectSet::PURE, Rollback::NoEffects),
			LogOp::Let(attrs) => match attrs.binding {
				LetBinding::Local => (EffectSet::PURE, Rollback::NoEffects),
				LetBinding::Session => (EffectSet::WRITES_SESSION, Rollback::Undone),
			},
			LogOp::BindStep(shape) if shape.writes() => (WRITE_SET, Rollback::Undone),
			LogOp::BindStep(shape) if shape.reads() => (EffectSet::READS_DATA, Rollback::NoEffects),
			LogOp::BindStep(..) => (EffectSet::PURE, Rollback::NoEffects),
			LogOp::Source(..) | LogOp::Fetch => (EffectSet::READS_DATA, Rollback::NoEffects),
			LogOp::Info {
				..
			} => (EffectSet::READS_CATALOG, Rollback::NoEffects),
			LogOp::Show(..) => {
				(EffectSet::READS_DATA.union(EffectSet::READS_CATALOG), Rollback::NoEffects)
			}
			// A notification unit reads the record's images, the records its `FETCH` reaches and
			// the subscriber's principal; the code of its clauses is judged through its regions.
			LogOp::Subscriber(..) => {
				(EffectSet::READS_DATA.union(EffectSet::READS_PRINCIPAL), Rollback::NoEffects)
			}
			LogOp::PermissionFilter {
				..
			}
			| LogOp::DdlGuard {
				..
			} => (EffectSet::READS_PRINCIPAL, Rollback::NoEffects),
			// A `USE` creates the namespace or database it selects when the principal may, so it
			// writes the catalog as well as the session.
			LogOp::Use {
				..
			} => (
				EffectSet::WRITES_SESSION
					.union(EffectSet::READS_PRINCIPAL)
					.union(EffectSet::CHANGES_CATALOG),
				Rollback::Survives,
			),
			LogOp::Option {
				..
			} => (EffectSet::WRITES_SESSION, Rollback::Survives),
			LogOp::Sleep => (EffectSet::BLOCKING, Rollback::NoEffects),
			// A write whose stages are linked runs the code its table stores, which no op of the
			// plan shows: a body whose effects are not stated, as a write that names no one table
			// runs.
			LogOp::Write(attrs) if attrs.linked.is_some() => (
				WRITE_SET.union(EffectSet::NOTIFY).union(EffectSet::OPAQUE_BODY),
				Rollback::Survives,
			),
			LogOp::Write(..) => (WRITE_SET.union(EffectSet::NOTIFY), Rollback::Survives),
			LogOp::WriteStage {
				..
			}
			| LogOp::Events(..)
			| LogOp::Assign(..)
			| LogOp::Unset {
				..
			}
			| LogOp::Content {
				..
			}
			| LogOp::MatchUpdate(..)
			| LogOp::MatchDelete {
				..
			}
			| LogOp::MatchInsert(..) => (WRITE_SET, Rollback::Undone),
			LogOp::Kill | LogOp::Live(..) => {
				(EffectSet::NOTIFY.union(EffectSet::WRITES_DATA), Rollback::Survives)
			}
			// The grants of an access method are catalog entries: a `SHOW` reads them, and a
			// `GRANT`, `REVOKE` or `PURGE` writes them, a `GRANT` drawing its key at random.
			LogOp::Access(attrs) => match attrs.op {
				AccessOp::Grant {
					..
				} => (ACCESS_WRITE.union(EffectSet::ENTROPY), Rollback::Undone),
				AccessOp::Revoke {
					..
				}
				| AccessOp::Purge {
					..
				} => (ACCESS_WRITE, Rollback::Undone),
				AccessOp::Show {
					..
				} => (EffectSet::READS_CATALOG, Rollback::NoEffects),
			},
			LogOp::Define(attrs) => ddl_class(define_extras(&attrs.object)),
			LogOp::Remove(attrs) => ddl_class(remove_extras(&attrs.object)),
			LogOp::Alter(attrs) => ddl_class(alter_extras(&attrs.object)),
			LogOp::Rebuild(..) => ddl_class(EffectSet::PURE),
		}
	}

	/// What each `CONTEXT` failure of the op rests on.
	fn context_deps(&self) -> SmallVec<[ContextDep; 1]> {
		match self {
			LogOp::PermissionFilter {
				..
			}
			| LogOp::Write(..)
			| LogOp::WriteStage {
				..
			}
			| LogOp::Events(..)
			| LogOp::Assign(..)
			| LogOp::Unset {
				..
			}
			| LogOp::Content {
				..
			}
			| LogOp::MatchUpdate(..)
			| LogOp::MatchDelete {
				..
			}
			| LogOp::MatchInsert(..) => {
				smallvec![ContextDep::PrincipalClass, ContextDep::PrincipalValue]
			}
			LogOp::BindStep(shape) if shape.writes() => {
				smallvec![ContextDep::PrincipalClass, ContextDep::PrincipalValue]
			}
			LogOp::DdlGuard {
				..
			} => smallvec![ContextDep::PrincipalClass, ContextDep::PrincipalRole],
			LogOp::Info {
				..
			} => smallvec![
				ContextDep::PrincipalClass,
				ContextDep::Backend {
					capability: "versions",
				}
			],
			LogOp::Define(..)
			| LogOp::Remove(..)
			| LogOp::Alter(..)
			| LogOp::Rebuild(..)
			| LogOp::Access(..)
			| LogOp::Kill
			| LogOp::Live(..)
			| LogOp::Subscriber(..)
			| LogOp::Show(..)
			| LogOp::Option {
				..
			}
			| LogOp::Use {
				..
			} => smallvec![ContextDep::PrincipalClass],
			LogOp::Sleep => smallvec![ContextDep::PrincipalRole],
			LogOp::VersionScope => smallvec![ContextDep::Backend {
				capability: "versions",
			}],
			LogOp::Request {
				..
			}
			| LogOp::Txn {
				..
			}
			| LogOp::Stmt {
				..
			}
			| LogOp::Statement {
				..
			}
			| LogOp::Source(..)
			| LogOp::Values {
				..
			}
			| LogOp::Match {
				..
			}
			| LogOp::MatchRead {
				..
			}
			| LogOp::MatchOutput(..)
			| LogOp::BindStep(..)
			| LogOp::Subquery
			| LogOp::UnionAll
			| LogOp::Join {
				..
			}
			| LogOp::Filter
			| LogOp::Split {
				..
			}
			| LogOp::Aggregate(..)
			| LogOp::Distinct
			| LogOp::Sort(..)
			| LogOp::Window(..)
			| LogOp::Limit {
				..
			}
			| LogOp::Project(..)
			| LogOp::Fetch
			| LogOp::Timeout
			| LogOp::Only
			| LogOp::Explain {
				..
			}
			| LogOp::Eval
			| LogOp::Block
			| LogOp::Let(..)
			| LogOp::Return
			| LogOp::Loop {
				..
			}
			| LogOp::If
			| LogOp::Choose(..)
			| LogOp::Yield
			| LogOp::Next
			| LogOp::Break => SmallVec::new(),
		}
	}

	/// The op's own failure classes. Total by type: no op is without an answer.
	///
	/// The answer is the class each op declares in the interface's per-tag table; a typed `LET`
	/// coerces its value and so can fail on it, an untyped one cannot.
	pub(crate) fn fallibility(&self, _cx: &EffectCx<'_>) -> Fallibility {
		match self {
			LogOp::Statement {
				served: Some(_),
				..
			} => Fallibility::TOP,
			LogOp::Loop {
				kind: LoopKind::GraphStep,
				..
			} => Fallibility::DATA,
			LogOp::Request {
				..
			}
			| LogOp::Stmt {
				..
			}
			| LogOp::Statement {
				..
			}
			| LogOp::Block
			| LogOp::If
			| LogOp::Subquery
			| LogOp::UnionAll
			| LogOp::Distinct
			| LogOp::Project(..)
			| LogOp::Filter
			| LogOp::Split {
				..
			}
			| LogOp::Join {
				..
			}
			| LogOp::Explain {
				..
			}
			| LogOp::Choose(..)
			| LogOp::Loop {
				..
			}
			| LogOp::Yield
			| LogOp::Next
			| LogOp::Break
			| LogOp::Return
			| LogOp::Values {
				..
			}
			| LogOp::Eval
			| LogOp::Timeout
			| LogOp::Match {
				..
			}
			| LogOp::MatchRead {
				..
			}
			| LogOp::MatchOutput(..) => Fallibility::INFALLIBLE,
			LogOp::Subscriber(..) => Fallibility::TOP,
			LogOp::BindStep(shape) if shape.writes() => Fallibility::TOP,
			LogOp::BindStep(shape) if shape.reads() => Fallibility::DATA,
			LogOp::BindStep(..) => Fallibility::INFALLIBLE,
			LogOp::Let(attrs) => match attrs.ty {
				Some(_) => Fallibility::VALUE,
				None => Fallibility::INFALLIBLE,
			},
			LogOp::Txn {
				..
			}
			| LogOp::Source(..)
			| LogOp::Fetch => Fallibility::DATA,
			LogOp::Limit {
				..
			}
			| LogOp::Aggregate(..)
			| LogOp::Sort(..)
			| LogOp::Window(..) => Fallibility::VALUE,
			LogOp::Only => Fallibility::VALUE.union(Fallibility::DATA),
			LogOp::VersionScope | LogOp::Sleep => Fallibility::VALUE.union(Fallibility::CONTEXT),
			LogOp::PermissionFilter {
				..
			}
			| LogOp::Use {
				..
			}
			| LogOp::Show(..) => Fallibility::CONTEXT.union(Fallibility::DATA),
			LogOp::Option {
				..
			}
			| LogOp::DdlGuard {
				..
			} => Fallibility::CONTEXT,
			LogOp::Write(..)
			| LogOp::WriteStage {
				..
			}
			| LogOp::Events(..)
			| LogOp::Define(..)
			| LogOp::Remove(..)
			| LogOp::Alter(..)
			| LogOp::Rebuild(..)
			| LogOp::Info {
				..
			}
			| LogOp::Access(..)
			| LogOp::Kill
			| LogOp::Live(..)
			| LogOp::MatchUpdate(..)
			| LogOp::MatchDelete {
				..
			}
			| LogOp::MatchInsert(..)
			| LogOp::Assign(..)
			| LogOp::Unset {
				..
			}
			| LogOp::Content {
				..
			} => Fallibility::TOP,
		}
	}

	/// Whether the op ends the block it stands last in.
	pub(crate) fn is_terminator(&self) -> bool {
		self.terminator().is_some()
	}

	/// How the op ends its block, or `None` for an op that is not a terminator.
	///
	/// The target of a `Return`, `Break` or `Next` is derived from the nesting of the regions by
	/// the container's control-target walk, so the answer names the kind with a null target.
	pub(crate) fn terminator(&self) -> Option<Terminator> {
		match self {
			LogOp::Yield => Some(Terminator::Yield),
			LogOp::Return => Some(Terminator::Return {
				target: RegionId::null(),
			}),
			LogOp::Break => Some(Terminator::Break {
				target: RegionId::null(),
			}),
			LogOp::Next => Some(Terminator::Next {
				target: RegionId::null(),
			}),
			LogOp::Request {
				..
			}
			| LogOp::Txn {
				..
			}
			| LogOp::Stmt {
				..
			}
			| LogOp::Statement {
				..
			}
			| LogOp::Source(..)
			| LogOp::Values {
				..
			}
			| LogOp::Match {
				..
			}
			| LogOp::MatchRead {
				..
			}
			| LogOp::MatchUpdate(..)
			| LogOp::MatchDelete {
				..
			}
			| LogOp::MatchInsert(..)
			| LogOp::MatchOutput(..)
			| LogOp::BindStep(..)
			| LogOp::Subquery
			| LogOp::Write(..)
			| LogOp::Assign(..)
			| LogOp::Unset {
				..
			}
			| LogOp::Content {
				..
			}
			| LogOp::DdlGuard {
				..
			}
			| LogOp::Define(..)
			| LogOp::Remove(..)
			| LogOp::Alter(..)
			| LogOp::Rebuild(..)
			| LogOp::UnionAll
			| LogOp::Join {
				..
			}
			| LogOp::Filter
			| LogOp::PermissionFilter {
				..
			}
			| LogOp::Split {
				..
			}
			| LogOp::Aggregate(..)
			| LogOp::Distinct
			| LogOp::Sort(..)
			| LogOp::Window(..)
			| LogOp::Limit {
				..
			}
			| LogOp::Project(..)
			| LogOp::Fetch
			| LogOp::Timeout
			| LogOp::VersionScope
			| LogOp::Only
			| LogOp::Explain {
				..
			}
			| LogOp::Eval
			| LogOp::Block
			| LogOp::Let(..)
			| LogOp::Loop {
				..
			}
			| LogOp::If
			| LogOp::Info {
				..
			}
			| LogOp::Sleep
			| LogOp::Use {
				..
			}
			| LogOp::Option {
				..
			}
			| LogOp::Access(..)
			| LogOp::Kill
			| LogOp::Live(..)
			| LogOp::Subscriber(..)
			| LogOp::Show(..)
			| LogOp::Choose(..)
			| LogOp::WriteStage {
				..
			}
			| LogOp::Events(..) => None,
		}
	}

	/// The kind of each of the op's regions, in position order.
	pub(crate) fn region_kinds(&self) -> RegionKinds {
		match self {
			LogOp::Request {
				..
			} => smallvec![RegionKind::Request],
			LogOp::Txn {
				mode,
			} => match mode {
				TxnRegionMode::Explicit {
					..
				} => smallvec![RegionKind::Transaction(TxnKind::Explicit)],
				TxnRegionMode::Session {
					..
				} => smallvec![RegionKind::Transaction(TxnKind::Session)],
			},
			LogOp::Stmt {
				commit,
				..
			} => smallvec![RegionKind::Statement(*commit)],
			LogOp::Statement {
				..
			} => smallvec![RegionKind::Statement(StatementCommit::InTxn)],
			LogOp::Block => smallvec![RegionKind::Block],
			LogOp::If => smallvec![RegionKind::StatementIf, RegionKind::StatementIf],
			LogOp::Loop {
				kind,
				step,
				..
			} => match kind {
				LoopKind::For => smallvec![RegionKind::StatementFor],
				LoopKind::Select
				| LoopKind::Write
				| LoopKind::Closure
				| LoopKind::ElementFilter
				| LoopKind::GraphStep
				| LoopKind::Recurse
				| LoopKind::Broadcast => {
					let mut kinds: RegionKinds = smallvec![RegionKind::Loop];
					if let Some(StepAttrs::Lookup(lookup)) = step.as_deref() {
						lookup_clause_kinds(lookup, &mut kinds);
					}
					kinds
				}
			},
			LogOp::Subquery => smallvec![RegionKind::Subquery],
			LogOp::Timeout => smallvec![RegionKind::Deadline],
			LogOp::Filter => smallvec![RegionKind::Clause(ClauseRole::Where)],
			LogOp::PermissionFilter {
				computed,
				..
			} => {
				let mut kinds: RegionKinds = smallvec![RegionKind::PermissionFrame];
				kinds.extend(std::iter::repeat_n(
					RegionKind::FieldFrame(FieldRole::Computed),
					computed.as_deref().map_or(0, ComputedReadsShape::derived_regions),
				));
				kinds
			}
			LogOp::Split {
				..
			} => smallvec![RegionKind::Clause(ClauseRole::Split)],
			LogOp::Fetch => smallvec![RegionKind::Clause(ClauseRole::Projection)],
			LogOp::Join {
				..
			} => smallvec![RegionKind::Clause(ClauseRole::Where)],
			LogOp::Aggregate(shape) => {
				let mut kinds: RegionKinds = std::iter::repeat_n(
					RegionKind::ErrorScope(ErrorScope::ShapeToNone),
					usize::from(shape.groups),
				)
				.collect();
				kinds.extend(std::iter::repeat_n(
					RegionKind::Clause(ClauseRole::Projection),
					field_regions(&shape.fields),
				));
				kinds
			}
			LogOp::Project(attrs) => {
				let fields = field_regions(&attrs.fields);
				let mut kinds: RegionKinds = SmallVec::new();
				kinds.extend(std::iter::repeat_n(
					RegionKind::Clause(ClauseRole::Projection),
					fields + usize::from(attrs.omit),
				));
				if attrs.layer {
					kinds.extend(std::iter::repeat_n(
						RegionKind::FieldFrame(FieldRole::Computed),
						fields + attrs.judged_regions(),
					));
				}
				kinds
			}
			LogOp::Sort(attrs) => match &attrs.ordering {
				SortShape::Random => SmallVec::new(),
				SortShape::Keys(keys) => {
					std::iter::repeat_n(RegionKind::Clause(ClauseRole::OrderKey), keys.len())
						.collect()
				}
			},
			LogOp::Window(shape) => {
				std::iter::repeat_n(RegionKind::Clause(ClauseRole::OrderKey), shape.key_regions())
					.chain(std::iter::repeat_n(
						RegionKind::Clause(ClauseRole::Projection),
						shape.arg_regions(),
					))
					.collect()
			}
			LogOp::Choose(spec) => {
				std::iter::repeat_n(RegionKind::ChooseArm, spec.arms.len()).collect()
			}
			LogOp::Match {
				..
			} => smallvec![RegionKind::Subquery],
			LogOp::MatchRead {
				..
			} => smallvec![RegionKind::Clause(ClauseRole::Where)],
			LogOp::MatchUpdate(attrs) => match &attrs.data {
				MatchData::Set(names) => {
					std::iter::repeat_n(RegionKind::Clause(ClauseRole::SetValue), names.len())
						.collect()
				}
				MatchData::Unset(_) => SmallVec::new(),
				MatchData::Content => smallvec![RegionKind::Clause(ClauseRole::Content)],
			},
			LogOp::BindStep(shape) => shape.region_kinds(),
			LogOp::MatchInsert(attrs) => std::iter::repeat_n(
				RegionKind::Clause(ClauseRole::Content),
				attrs.nodes.len() + attrs.edges.len(),
			)
			.collect(),
			LogOp::MatchOutput(shape) => {
				let mut kinds: RegionKinds = std::iter::repeat_n(
					RegionKind::Clause(ClauseRole::Projection),
					shape.columns.len(),
				)
				.collect();
				kinds.push(RegionKind::Clause(ClauseRole::GroupKey));
				kinds.extend(std::iter::repeat_n(
					RegionKind::Clause(ClauseRole::OrderKey),
					shape.order.len(),
				));
				kinds
			}
			LogOp::Write(attrs) => attrs.region_kinds().into_iter().collect(),
			LogOp::Define(attrs) => {
				let mut slots = Slots::new();
				define_slots(&attrs.object, &mut slots);
				kinds_by_slot(slots)
			}
			LogOp::Alter(attrs) => {
				let mut slots = Slots::new();
				alter_slots(&attrs.object, &mut slots);
				kinds_by_slot(slots)
			}
			LogOp::Live(attrs) => match attrs.fields {
				LiveShape::Select => smallvec![RegionKind::StoredClause(StoredRole::LiveFields)],
				LiveShape::Diff => SmallVec::new(),
			},
			LogOp::Subscriber(attrs) => attrs.region_kinds().into_iter().collect(),
			LogOp::Access(attrs) => match attrs.op {
				AccessOp::Show {
					..
				}
				| AccessOp::Revoke {
					..
				} => smallvec![RegionKind::Clause(ClauseRole::Where)],
				AccessOp::Grant {
					..
				}
				| AccessOp::Purge {
					..
				} => SmallVec::new(),
			},
			LogOp::WriteStage {
				..
			} => smallvec![RegionKind::WriteStage],
			LogOp::Events(attrs) => attrs.region_kinds().into_iter().collect(),
			LogOp::Source(..)
			| LogOp::Values {
				..
			}
			| LogOp::MatchDelete {
				..
			}
			| LogOp::Assign(..)
			| LogOp::Unset {
				..
			}
			| LogOp::Content {
				..
			}
			| LogOp::DdlGuard {
				..
			}
			| LogOp::Remove(..)
			| LogOp::Rebuild(..)
			| LogOp::UnionAll
			| LogOp::Distinct
			| LogOp::Limit {
				..
			}
			| LogOp::VersionScope
			| LogOp::Only
			| LogOp::Explain {
				..
			}
			| LogOp::Eval
			| LogOp::Let(..)
			| LogOp::Return
			| LogOp::Info {
				..
			}
			| LogOp::Sleep
			| LogOp::Use {
				..
			}
			| LogOp::Option {
				..
			}
			| LogOp::Kill
			| LogOp::Show(..)
			| LogOp::Yield
			| LogOp::Next
			| LogOp::Break => SmallVec::new(),
		}
	}

	/// The operand and result counts the op accepts.
	///
	/// A statement or transaction op has one result, the statement's slot value. An op that stands
	/// inside a clause region of a write or a `MATCH` stage produces no result.
	pub(crate) fn arity(&self) -> Arity {
		let exactly = |operands: u16, results: u16| Arity {
			operands: OperandRule::Exactly(operands),
			results,
		};
		let at_least = |operands: u16, results: u16| Arity {
			operands: OperandRule::AtLeast(operands),
			results,
		};
		match self {
			LogOp::Request {
				..
			}
			| LogOp::Txn {
				..
			}
			| LogOp::Stmt {
				..
			}
			| LogOp::Statement {
				..
			}
			| LogOp::Match {
				..
			}
			| LogOp::Subquery
			| LogOp::Block
			| LogOp::Show(..)
			| LogOp::Option {
				..
			} => exactly(0, 1),
			LogOp::Source(attrs) => match attrs.source_ref {
				SourceRefShape::Table(_) => exactly(0, 1),
				SourceRefShape::RecordId
				| SourceRefShape::Mock
				| SourceRefShape::Param
				| SourceRefShape::Dynamic => exactly(1, 1),
			},
			LogOp::Loop {
				sink: LoopSink::Fold,
				..
			} => at_least(1, 1),
			LogOp::Loop {
				step: Some(step),
				..
			} if step.takes_clauses() => at_least(1, 1),
			LogOp::Values {
				..
			}
			| LogOp::If
			| LogOp::Loop {
				..
			}
			| LogOp::Eval
			| LogOp::Let(..)
			| LogOp::Filter
			| LogOp::PermissionFilter {
				..
			}
			| LogOp::Split {
				..
			}
			| LogOp::Aggregate(..)
			| LogOp::Distinct
			| LogOp::Sort(..)
			| LogOp::Window(..)
			| LogOp::Project(..)
			| LogOp::Fetch
			| LogOp::Only
			| LogOp::Explain {
				..
			}
			| LogOp::Timeout
			| LogOp::Kill
			| LogOp::Live(..)
			| LogOp::Sleep => exactly(1, 1),
			LogOp::Subscriber(..) => exactly(0, 0),
			LogOp::Return => exactly(1, 0),
			LogOp::Yield => at_least(0, 0),
			LogOp::Next
			| LogOp::Break
			| LogOp::WriteStage {
				..
			}
			| LogOp::Events(..) => exactly(0, 0),
			LogOp::Choose(..) => at_least(0, 1),
			LogOp::Join {
				..
			}
			| LogOp::VersionScope => exactly(2, 1),
			LogOp::UnionAll
			| LogOp::Limit {
				..
			}
			| LogOp::Write(..) => at_least(1, 1),
			LogOp::Assign(..) => at_least(1, 0),
			LogOp::Unset {
				..
			} => at_least(0, 0),
			LogOp::Content {
				..
			} => exactly(1, 0),
			LogOp::DdlGuard {
				..
			} => at_least(0, 0),
			LogOp::Define(..) | LogOp::Remove(..) | LogOp::Alter(..) | LogOp::Rebuild(..) => {
				at_least(0, 1)
			}
			LogOp::Info {
				subject,
				..
			} => match subject {
				InfoSubject::Root | InfoSubject::Ns | InfoSubject::Db => at_least(0, 1),
				InfoSubject::Table
				| InfoSubject::User {
					..
				} => at_least(1, 1),
				InfoSubject::Index => at_least(2, 1),
			},
			LogOp::Access(attrs) => match &attrs.op {
				AccessOp::Grant {
					subject,
				} => match subject {
					AccessSubjectShape::Record => at_least(1, 1),
					AccessSubjectShape::User(_) => at_least(0, 1),
				},
				AccessOp::Show {
					..
				}
				| AccessOp::Revoke {
					..
				}
				| AccessOp::Purge {
					..
				} => at_least(0, 1),
			},
			LogOp::Use {
				target,
			} => match target {
				UseTarget::Ns | UseTarget::Db => exactly(1, 1),
				UseTarget::NsDb => exactly(2, 1),
				UseTarget::Default => exactly(0, 1),
			},
			LogOp::MatchRead {
				..
			}
			| LogOp::MatchDelete {
				..
			}
			| LogOp::MatchUpdate(..)
			| LogOp::MatchInsert(..) => exactly(0, 0),
			LogOp::MatchOutput(..) => at_least(0, 0),
			LogOp::BindStep(shape) => exactly(shape.operands(), 1),
		}
	}

	/// How the op reaches the interpreter on the target `isa` describes.
	///
	/// A logical op is converted to the physical dialect before lowering, so the answer for every
	/// variant is a call of a unit and never reaches the interpreter as an instruction.
	pub(crate) fn lowers_natively(&self, _isa: IsaCaps) -> NativeLowering {
		match self {
			LogOp::Request {
				..
			}
			| LogOp::Txn {
				..
			}
			| LogOp::Stmt {
				..
			}
			| LogOp::Statement {
				..
			}
			| LogOp::Source(..)
			| LogOp::Values {
				..
			}
			| LogOp::Match {
				..
			}
			| LogOp::MatchRead {
				..
			}
			| LogOp::MatchUpdate(..)
			| LogOp::MatchDelete {
				..
			}
			| LogOp::MatchInsert(..)
			| LogOp::MatchOutput(..)
			| LogOp::BindStep(..)
			| LogOp::Subquery
			| LogOp::Write(..)
			| LogOp::Assign(..)
			| LogOp::Unset {
				..
			}
			| LogOp::Content {
				..
			}
			| LogOp::DdlGuard {
				..
			}
			| LogOp::Define(..)
			| LogOp::Remove(..)
			| LogOp::Alter(..)
			| LogOp::Rebuild(..)
			| LogOp::UnionAll
			| LogOp::Join {
				..
			}
			| LogOp::Filter
			| LogOp::PermissionFilter {
				..
			}
			| LogOp::Split {
				..
			}
			| LogOp::Aggregate(..)
			| LogOp::Distinct
			| LogOp::Sort(..)
			| LogOp::Window(..)
			| LogOp::Limit {
				..
			}
			| LogOp::Project(..)
			| LogOp::Fetch
			| LogOp::Timeout
			| LogOp::VersionScope
			| LogOp::Only
			| LogOp::Explain {
				..
			}
			| LogOp::Eval
			| LogOp::Block
			| LogOp::Let(..)
			| LogOp::Return
			| LogOp::Loop {
				..
			}
			| LogOp::If
			| LogOp::Info {
				..
			}
			| LogOp::Sleep
			| LogOp::Use {
				..
			}
			| LogOp::Option {
				..
			}
			| LogOp::Access(..)
			| LogOp::Kill
			| LogOp::Live(..)
			| LogOp::Subscriber(..)
			| LogOp::Show(..)
			| LogOp::Choose(..)
			| LogOp::WriteStage {
				..
			}
			| LogOp::Events(..)
			| LogOp::Yield
			| LogOp::Next
			| LogOp::Break => NativeLowering::UnitCall,
		}
	}
}

/// The footprint of an op with the given effects. It reads `Unknown` when the set holds any
/// effect. It writes `Unknown` when the set changes state that outlives the op
/// ([`EffectSet::mutates_state`]) or runs a body nothing is known about
/// ([`EffectSet::OPAQUE_BODY`]), and writes nothing otherwise: an op that only reads, draws or
/// waits leaves every resource as it found it.
fn footprint_of(set: EffectSet) -> IrFootprint {
	let unknown = || smallvec![IrResource::Unknown];
	IrFootprint {
		reads: if set.is_pure() {
			SmallVec::new()
		} else {
			unknown()
		},
		writes: if set.mutates_state() || set.contains(EffectSet::OPAQUE_BODY) {
			unknown()
		} else {
			SmallVec::new()
		},
	}
}

/// How far two evaluations of an op with the given effects may be treated as one.
///
/// An op whose set is not deterministic (it writes, waits, draws randomness, notifies or changes
/// the session or catalog) is never one value twice. A read of stored data or of the catalog holds
/// within one statement only.
fn determinism_of(set: EffectSet) -> Determinism {
	if !set.is_deterministic() {
		Determinism::NonDeterministic
	} else if set.intersects(EffectSet::READS_DATA.union(EffectSet::READS_CATALOG)) {
		Determinism::WithinStatementUnlessWritten
	} else {
		Determinism::Deterministic
	}
}

/// The flags and rollback of a schema statement whose form adds `extras` to the catalog change.
///
/// An id drawn from a counter and a notification survive a rollback; the catalog change and the
/// queued work of a `REMOVE` do not.
fn ddl_class(extras: EffectSet) -> (EffectSet, Rollback) {
	let set = DDL_SET.union(extras);
	let survives = set.intersects(EffectSet::ADVANCES_STATE.union(EffectSet::NOTIFY));
	(
		set,
		if survives {
			Rollback::Survives
		} else {
			Rollback::Undone
		},
	)
}

/// What a `DEFINE` of this form does beyond changing the catalog: draw an id from a counter, or
/// draw randomness for a credential or a key. A view also reads the tables it is defined over
/// and writes the rows that fill it.
fn define_extras(object: &DefineObject) -> EffectSet {
	match object {
		DefineObject::Table(table) if table.view.is_some() => {
			EffectSet::ADVANCES_STATE.union(EffectSet::READS_DATA).union(EffectSet::WRITES_DATA)
		}
		DefineObject::Namespace {
			..
		}
		| DefineObject::Database {
			..
		}
		| DefineObject::Table(..) => EffectSet::ADVANCES_STATE,
		DefineObject::User(..) | DefineObject::Access(..) => EffectSet::ENTROPY,
		DefineObject::Function(..)
		| DefineObject::Analyzer {
			..
		}
		| DefineObject::Param {
			..
		}
		| DefineObject::Event {
			..
		}
		| DefineObject::Field(..)
		| DefineObject::Index {
			..
		}
		| DefineObject::Model {
			..
		}
		| DefineObject::Module {
			..
		}
		| DefineObject::Config(..)
		| DefineObject::Api(..)
		| DefineObject::Bucket {
			..
		}
		| DefineObject::Type {
			..
		}
		| DefineObject::Sequence {
			..
		} => EffectSet::PURE,
	}
}

/// What a `REMOVE` of this form does beyond changing the catalog: queue the reclaim of the
/// object's indexes, and end the live queries on it.
fn remove_extras(object: &RemoveObject) -> EffectSet {
	match object {
		RemoveObject::Namespace {
			..
		}
		| RemoveObject::Database {
			..
		}
		| RemoveObject::Table {
			..
		} => EffectSet::DEFERS.union(EffectSet::NOTIFY),
		RemoveObject::Index {
			..
		} => EffectSet::DEFERS,
		RemoveObject::Function {
			..
		}
		| RemoveObject::Analyzer {
			..
		}
		| RemoveObject::Access {
			..
		}
		| RemoveObject::Param {
			..
		}
		| RemoveObject::Event {
			..
		}
		| RemoveObject::Field {
			..
		}
		| RemoveObject::User {
			..
		}
		| RemoveObject::Model {
			..
		}
		| RemoveObject::Api {
			..
		}
		| RemoveObject::Bucket {
			..
		}
		| RemoveObject::Type {
			..
		}
		| RemoveObject::Sequence {
			..
		}
		| RemoveObject::Module {
			..
		}
		| RemoveObject::Config {
			..
		} => EffectSet::PURE,
	}
}

/// What an `ALTER` of this form does beyond changing the catalog: draw randomness to hash a new
/// password.
fn alter_extras(object: &AlterObject) -> EffectSet {
	match object {
		AlterObject::User(user) if user.password.is_some() => EffectSet::ENTROPY,
		AlterObject::User(..)
		| AlterObject::System {
			..
		}
		| AlterObject::Namespace {
			..
		}
		| AlterObject::Database {
			..
		}
		| AlterObject::Table(..)
		| AlterObject::Api {
			..
		}
		| AlterObject::Event(..)
		| AlterObject::Index {
			..
		}
		| AlterObject::Sequence {
			..
		}
		| AlterObject::Field(..)
		| AlterObject::Param {
			..
		}
		| AlterObject::Bucket {
			..
		}
		| AlterObject::Type {
			..
		}
		| AlterObject::Config {
			..
		}
		| AlterObject::Analyzer {
			..
		}
		| AlterObject::Function(..)
		| AlterObject::Access(..)
		| AlterObject::Module {
			..
		} => EffectSet::PURE,
	}
}

// The number of `Clause(Projection)` regions a projection list holds: one per field that is not
// `*`.
/// Appends the kinds of the clause regions of a lookup, which follow its body: the predicate, the
/// splits, the group keys, the order keys and the columns of its projection.
pub(crate) fn lookup_clause_kinds(lookup: &LookupAttrs, kinds: &mut RegionKinds) {
	if lookup.cond {
		kinds.push(RegionKind::Clause(ClauseRole::Where));
	}
	kinds.extend(std::iter::repeat_n(
		RegionKind::Clause(ClauseRole::Split),
		usize::from(lookup.split.unwrap_or(0)),
	));
	kinds.extend(std::iter::repeat_n(
		RegionKind::ErrorScope(ErrorScope::ShapeToNone),
		usize::from(lookup.group.unwrap_or(0)),
	));
	if let Some(SortShape::Keys(keys)) = &lookup.order {
		kinds.extend(std::iter::repeat_n(RegionKind::Clause(ClauseRole::OrderKey), keys.len()));
	}
	if let Some(fields) = &lookup.fields {
		kinds.extend(std::iter::repeat_n(
			RegionKind::Clause(ClauseRole::Projection),
			field_regions(fields),
		));
	}
}

fn field_regions(fields: &FieldsShape) -> usize {
	match fields {
		FieldsShape::Value {
			..
		} => 1,
		FieldsShape::Select(list) => list
			.iter()
			.filter(|field| match field {
				FieldShape::All => false,
				FieldShape::Single {
					..
				} => true,
			})
			.count(),
	}
}

/// The kinds of a region list from the (slot, kind) pairs the attributes name, in slot order.
fn kinds_by_slot(mut slots: Slots) -> RegionKinds {
	slots.sort_by_key(|(slot, _)| *slot);
	slots.into_iter().map(|(_, kind)| kind).collect()
}

fn stored(slots: &mut Slots, slot: RegionSlot, role: StoredRole) {
	slots.push((slot, RegionKind::StoredClause(role)));
}

fn permission_slot(slots: &mut Slots, shape: PermissionShape) {
	match shape {
		PermissionShape::None | PermissionShape::Full => {}
		PermissionShape::Specific(slot) => stored(slots, slot, StoredRole::Permissions),
	}
}

fn permissions_slots(slots: &mut Slots, shape: &PermissionsShape) {
	for clause in [shape.select, shape.create, shape.update, shape.delete] {
		permission_slot(slots, clause);
	}
}

fn reference_slot(slots: &mut Slots, reference: ReferenceShape) {
	match reference.on_delete {
		OnDeleteShape::Custom(slot) => stored(slots, slot, StoredRole::Then),
		OnDeleteShape::Reject
		| OnDeleteShape::Ignore
		| OnDeleteShape::Cascade
		| OnDeleteShape::Unset => {}
	}
}

fn api_config_slots(slots: &mut Slots, config: &ApiConfigShape) {
	permission_slot(slots, config.permissions);
}

fn config_slots(slots: &mut Slots, config: &ConfigShape) {
	match config {
		ConfigShape::GraphQL(_) | ConfigShape::Default(_) => {}
		ConfigShape::Api(api) => api_config_slots(slots, api),
	}
}

fn altered<T>(kind: &AlterKind<T>, mut set: impl FnMut(&T)) {
	match kind {
		AlterKind::Set(value) => set(value),
		AlterKind::None | AlterKind::Drop => {}
	}
}

/// The region each stored clause of a `DEFINE` fills, by the slot its attributes name.
fn define_slots(object: &DefineObject, slots: &mut Slots) {
	match object {
		DefineObject::Namespace {
			..
		}
		| DefineObject::Database {
			..
		}
		| DefineObject::Analyzer {
			..
		}
		| DefineObject::User(..)
		| DefineObject::Sequence {
			..
		} => {}
		DefineObject::Function(function) => {
			stored(slots, function.body, StoredRole::Body);
			permission_slot(slots, function.permissions);
		}
		DefineObject::Param {
			permissions,
			..
		}
		| DefineObject::Model {
			permissions,
			..
		}
		| DefineObject::Module {
			permissions,
			..
		}
		| DefineObject::Bucket {
			permissions,
			..
		}
		| DefineObject::Type {
			permissions,
			..
		} => permission_slot(slots, *permissions),
		DefineObject::Table(table) => {
			if let Some(view) = &table.view {
				stored(slots, view.query, StoredRole::View);
				if let Some(backfill) = view.backfill {
					slots.push((backfill, RegionKind::Subquery));
				}
				for field in view.group_fields.iter() {
					slots.push((*field, RegionKind::Clause(ClauseRole::Projection)));
				}
			}
			permissions_slots(slots, &table.permissions);
		}
		DefineObject::Event {
			when,
			then,
			..
		} => {
			stored(slots, *when, StoredRole::When);
			for slot in then.iter() {
				stored(slots, *slot, StoredRole::Then);
			}
		}
		DefineObject::Field(field) => {
			if let Some(slot) = field.value {
				stored(slots, slot, StoredRole::Value);
			}
			if let Some(slot) = field.assert {
				stored(slots, slot, StoredRole::Assert);
			}
			if let Some(slot) = field.computed {
				stored(slots, slot, StoredRole::Computed);
			}
			match field.default {
				DefaultShape::None => {}
				DefaultShape::Always(slot) | DefaultShape::Set(slot) => {
					stored(slots, slot, StoredRole::Default)
				}
			}
			permissions_slots(slots, &field.permissions);
			if let Some(reference) = &field.reference {
				reference_slot(slots, *reference);
			}
		}
		DefineObject::Index {
			columns,
			index,
			..
		} => {
			for column in columns.iter() {
				match column {
					IndexColumn::Path(_) => {}
					IndexColumn::Stored(slot) => stored(slots, *slot, StoredRole::IndexColumn),
				}
			}
			match index {
				IndexSpec::Count(Some(slot)) => stored(slots, *slot, StoredRole::When),
				IndexSpec::Count(None)
				| IndexSpec::Idx
				| IndexSpec::Uniq
				| IndexSpec::Hnsw(_)
				| IndexSpec::FullText(_)
				| IndexSpec::DiskAnn(_)
				| IndexSpec::Spatial => {}
			}
		}
		DefineObject::Access(access) => {
			if let Some(slot) = access.authenticate {
				stored(slots, slot, StoredRole::Authenticate);
			}
			if let Some(slot) = access.context {
				stored(slots, slot, StoredRole::Context);
			}
			match &access.access_type {
				AccessTypeShape::Record(record) => {
					if let Some(slot) = record.signup {
						stored(slots, slot, StoredRole::Signup);
					}
					if let Some(slot) = record.signin {
						stored(slots, slot, StoredRole::Signin);
					}
				}
				AccessTypeShape::Jwt(_) | AccessTypeShape::Bearer(_) => {}
			}
		}
		DefineObject::Config(config) => config_slots(slots, config),
		DefineObject::Api(api) => {
			for action in api.actions.iter() {
				stored(slots, action.action, StoredRole::Action);
				api_config_slots(slots, &action.config);
			}
			if let Some(slot) = api.fallback {
				stored(slots, slot, StoredRole::Fallback);
			}
			api_config_slots(slots, &api.config);
		}
	}
}

/// The region each stored clause of an `ALTER` fills, by the slot its attributes name.
fn alter_slots(object: &AlterObject, slots: &mut Slots) {
	match object {
		AlterObject::System {
			..
		}
		| AlterObject::Namespace {
			..
		}
		| AlterObject::Database {
			..
		}
		| AlterObject::Index {
			..
		}
		| AlterObject::Sequence {
			..
		}
		| AlterObject::Analyzer {
			..
		}
		| AlterObject::Type {
			..
		}
		| AlterObject::User(..) => {}
		AlterObject::Table(table) => {
			if let Some(permissions) = &table.permissions {
				permissions_slots(slots, permissions);
			}
		}
		AlterObject::Api {
			clauses,
			..
		} => {
			for clause in clauses.iter() {
				match clause {
					AlterApiClauseShape::ForAny {
						config,
						fallback,
					} => {
						if let Some(config) = config {
							api_config_slots(slots, config);
						}
						altered(fallback, |slot| stored(slots, *slot, StoredRole::Fallback));
					}
					AlterApiClauseShape::SetAction(action) => {
						stored(slots, action.action, StoredRole::Action);
						api_config_slots(slots, &action.config);
					}
					AlterApiClauseShape::DropAction {
						..
					} => {}
				}
			}
		}
		AlterObject::Event(event) => {
			altered(&event.when, |slot| stored(slots, *slot, StoredRole::When));
			altered(&event.then, |list| {
				for slot in list.iter() {
					stored(slots, *slot, StoredRole::Then);
				}
			});
		}
		AlterObject::Field(field) => {
			altered(&field.value, |slot| stored(slots, *slot, StoredRole::Value));
			altered(&field.assert, |slot| stored(slots, *slot, StoredRole::Assert));
			match field.default {
				AlterDefaultShape::None | AlterDefaultShape::Drop => {}
				AlterDefaultShape::Always(slot) | AlterDefaultShape::Set(slot) => {
					stored(slots, slot, StoredRole::Default)
				}
			}
			if let Some(permissions) = &field.permissions {
				permissions_slots(slots, permissions);
			}
			altered(&field.reference, |reference| reference_slot(slots, *reference));
		}
		AlterObject::Param {
			permissions,
			..
		}
		| AlterObject::Bucket {
			permissions,
			..
		}
		| AlterObject::Module {
			permissions,
			..
		} => {
			if let Some(permissions) = permissions {
				permission_slot(slots, *permissions);
			}
		}
		AlterObject::Config {
			inner,
			..
		} => config_slots(slots, inner),
		AlterObject::Function(function) => {
			altered(&function.block, |slot| stored(slots, *slot, StoredRole::Body));
			if let Some(permissions) = &function.permissions {
				permission_slot(slots, *permissions);
			}
		}
		AlterObject::Access(access) => {
			altered(&access.authenticate, |slot| stored(slots, *slot, StoredRole::Authenticate));
			altered(&access.context, |slot| stored(slots, *slot, StoredRole::Context));
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::iface::effects::IrResource;

	/// An op that only reads states unknown reads and no writes; an op that may leave something
	/// behind or runs a body the plan does not show states unknown writes as well; a pure op
	/// states neither.
	#[test]
	fn a_footprint_writes_only_what_the_effects_may_write() {
		let unknown = [IrResource::Unknown];
		let reads_only = [
			EffectSet::READS_DATA,
			EffectSet::READS_CATALOG,
			EffectSet::READS_EXTERNAL,
			EffectSet::READS_ROW,
			EffectSet::READS_PRINCIPAL,
			EffectSet::READS_SESSION_DB,
			EffectSet::ENTROPY,
			EffectSet::CLOCK,
			EffectSet::BLOCKING,
			EffectSet::MAY_REJECT,
			EffectSet::READS_DATA.union(EffectSet::READS_CATALOG),
		];
		for set in reads_only {
			let footprint = footprint_of(set);
			assert_eq!(footprint.reads.as_slice(), &unknown, "{set:?}");
			assert!(footprint.writes.is_empty(), "{set:?}");
		}
		let writes = [
			EffectSet::WRITES_DATA,
			EffectSet::WRITES_EXTERNAL,
			EffectSet::ADVANCES_STATE,
			EffectSet::WRITES_SESSION,
			EffectSet::NOTIFY,
			EffectSet::DEFERS,
			EffectSet::CHANGES_CATALOG,
			EffectSet::OPAQUE_BODY,
			EffectSet::READS_DATA.union(EffectSet::WRITES_DATA),
			EffectSet::TOP,
		];
		for set in writes {
			let footprint = footprint_of(set);
			assert_eq!(footprint.reads.as_slice(), &unknown, "{set:?}");
			assert_eq!(footprint.writes.as_slice(), &unknown, "{set:?}");
		}
		let pure = footprint_of(EffectSet::PURE);
		assert!(pure.reads.is_empty() && pure.writes.is_empty());
	}
}
