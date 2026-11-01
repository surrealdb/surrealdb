//! The physical dialect: where a statement runs and by which method.
//!
//! A `phys` op names an access path, a join or sort method, a pipeline or a guarded choice. A
//! placement decision is derived from the logical facts and the catalog's statistics, is held in
//! the container's placement table, and is never read back into the logical dialect.
//!
//! A physical op's effects and failure classes are not declared here: they are derived from the
//! logical ops the op stands for, through the physical-effects rule of the analysis module. The
//! structural answers below state the shape of each op:
//!
//! - `terminator` names `Yield` for `phys.yield`. `Return`, `Break` and `Next` are terminators
//!   whose target region needs the container, so they answer `None` here and the verifier's target
//!   rule owns their targets.
//! - `region_kinds` answers the kind of each region the attributes fix, in position order. A
//!   statement-family op has the regions of the logical op with the same attributes. A count the
//!   attributes state (a `MATCH`'s stages, a read stage's predicates, an aggregation's group keys,
//!   an `IF`'s and a `FOR`'s position) is exact. Where no attribute fixes a region count, the
//!   answer is the minimal form: one region of each listed kind. A region whose presence only the
//!   plan's construction decides (a fallback plan, a `once` predicate) is not listed.
//! - `arity` answers the operand rule and the result count; for an op whose operand count depends
//!   on its attributes the rule is the one those attributes fix.
//! - `lowers_natively` answers `Native` only for an op whose lowered instructions the leaf profile
//!   classifies as leaves, and `UnitCall` for every other op.
//!
//! [`plan_physical`] is the phase entry: it converts a container to the physical dialect, collects
//! the plan-time values the conversion's decisions consumed, builds the pipelines, derives the
//! pipeline graph and decides every pipeline's placement.

use slotmap::Key;
use smallvec::{SmallVec, smallvec};

pub(crate) mod access;
pub(crate) mod access_expr;
mod access_read;
mod access_regions;
pub(crate) mod callee;
pub(crate) mod catalog_effects;
mod chain;
pub(crate) mod choose;
pub(crate) mod conjunct;
pub(crate) mod conversion;
pub(crate) mod cost;
mod count;
pub(crate) mod digest;
mod duplicate;
mod edit;
mod existence;
mod extreme;
pub(crate) mod facts;
pub(crate) mod frontier;
mod hold_left;
pub(crate) mod hop_key;
mod index_answer;
pub(crate) mod io;
mod join_lookup;
mod keyed_record;
mod knn;
mod knn_ids;
pub(crate) mod matches;
pub(crate) mod nested;
mod offer;
mod once;
pub(crate) mod ops;
pub(crate) mod order;
pub(crate) mod pipeline;
pub(crate) mod placement;
pub(crate) mod placement_attr;
mod prefilter;
pub(crate) mod print;
mod recheck;
pub(crate) mod reg;
pub(crate) mod rewrite;
pub(crate) mod schedule;
pub(crate) mod seed_host;
#[cfg(all(test, feature = "kv-mem"))]
mod tests_write_prune;
mod topk_packed;
pub(crate) mod verify;
pub(crate) mod write_cut;
mod write_prune;

use ops::{
	ComputedReadsShape, CountShape, KnnTargetShape, MatchStepAttrs, OmissionShape, ProjectionShape,
	SortKeyReadShape, WalkActionShape, WalkStepShape, WriteSubjectsShape,
};
#[allow(unused_imports)]
pub(crate) use ops::{PhysOp, PhysTag};
#[allow(unused_imports)]
pub(crate) use placement_attr::{PlacementAttr, PlacementTable};

use super::cache::deps::ValueDeps;
use super::core::convert::{ConversionReport, convert};
use super::core::facts::FactCache;
use super::core::ir::Ir;
use super::error::IrError;
use super::iface::catalog::PhysCx;
use super::iface::conversion::PHYSICAL_PHASE;
use super::iface::fold::PlanTimeFolder;
use super::iface::ids::RegionId;
use super::iface::region_kind::{
	ClauseRole, ErrorScope, FieldRole, RegionKind, RegionKinds, StatementCommit, StoredRole,
	TxnKind,
};
use super::iface::statements::{AccessOp, AccessSubjectShape, InfoSubject, LiveShape, MatchData};
use super::logical::effects::lookup_clause_kinds;
use super::logical::ops::{
	FieldShape, FieldsShape, LogOp, LoopKind, SourceRefShape, StepAttrs, TxnRegionMode, UseTarget,
};
use super::op_kind::{Arity, IsaCaps, LeafClass, NativeLowering, OperandRule, Terminator};
use super::registry;
use crate::exec::ir::expr::ops::DestructureEntry;

#[deny(clippy::wildcard_enum_match_arm)]
impl PhysOp {
	/// Whether the op ends the block it stands last in: `Yield`, `Return`, `Break` and `Next`.
	pub(crate) fn is_terminator(&self) -> bool {
		self.terminator().is_some()
	}

	/// How the op ends its block, or `None` for an op that is not a terminator.
	///
	/// The target of a `Return`, `Break` or `Next` is derived from the nesting of the regions by
	/// the container's control-target walk, so the answer names the kind with a null target.
	pub(crate) fn terminator(&self) -> Option<Terminator> {
		match self {
			PhysOp::Yield => Some(Terminator::Yield),
			PhysOp::Return => Some(Terminator::Return {
				target: RegionId::null(),
			}),
			PhysOp::Break => Some(Terminator::Break {
				target: RegionId::null(),
			}),
			PhysOp::Next => Some(Terminator::Next {
				target: RegionId::null(),
			}),
			PhysOp::TableScan(..)
			| PhysOp::IndexScan(..)
			| PhysOp::IndexUnion(..)
			| PhysOp::Bitmap(..)
			| PhysOp::IndexLookup(..)
			| PhysOp::FullTextScan(..)
			| PhysOp::KeyRangeScan(..)
			| PhysOp::KnnScan(..)
			| PhysOp::KnnTopK(..)
			| PhysOp::Count(..)
			| PhysOp::RecordScan(..)
			| PhysOp::Vacant {
				..
			}
			| PhysOp::Enforced {
				..
			}
			| PhysOp::Values {
				..
			}
			| PhysOp::Subquery
			| PhysOp::Probe {
				..
			}
			| PhysOp::Union
			| PhysOp::Join(..)
			| PhysOp::Distinct {
				..
			}
			| PhysOp::PermissionFilter(..)
			| PhysOp::Layer(..)
			| PhysOp::Filter
			| PhysOp::Fetch
			| PhysOp::Split {
				..
			}
			| PhysOp::Sort(..)
			| PhysOp::Window(..)
			| PhysOp::Aggregate(..)
			| PhysOp::Limit {
				..
			}
			| PhysOp::Project(..)
			| PhysOp::Eval
			| PhysOp::Timeout
			| PhysOp::VersionScope
			| PhysOp::Only
			| PhysOp::Explain {
				..
			}
			| PhysOp::Assign(..)
			| PhysOp::Unset {
				..
			}
			| PhysOp::Content {
				..
			}
			| PhysOp::Request {
				..
			}
			| PhysOp::Txn {
				..
			}
			| PhysOp::Stmt {
				..
			}
			| PhysOp::Statement {
				..
			}
			| PhysOp::Match {
				..
			}
			| PhysOp::MatchStep(..)
			| PhysOp::DdlGuard {
				..
			}
			| PhysOp::Define(..)
			| PhysOp::Remove(..)
			| PhysOp::Alter(..)
			| PhysOp::Rebuild(..)
			| PhysOp::Info {
				..
			}
			| PhysOp::Use {
				..
			}
			| PhysOp::Option {
				..
			}
			| PhysOp::Access(..)
			| PhysOp::Kill
			| PhysOp::Live(..)
			| PhysOp::Subscriber(..)
			| PhysOp::Show(..)
			| PhysOp::Write(..)
			| PhysOp::WriteStage {
				..
			}
			| PhysOp::Events(..)
			| PhysOp::Block
			| PhysOp::Let(..)
			| PhysOp::Sleep
			| PhysOp::If {
				..
			}
			| PhysOp::Loop {
				..
			}
			| PhysOp::Pipeline(..)
			| PhysOp::Choose(..)
			| PhysOp::Prefetch(..)
			| PhysOp::Materialize {
				..
			}
			| PhysOp::LoadUnit(..) => None,
		}
	}

	/// The kinds the op may own beyond the regions [`PhysOp::region_kinds`] names: the clause
	/// regions of the write stage, assignment or live query it was converted from, which the
	/// logical op hosts the same way, and the predicate a filter judges once.
	pub(crate) fn tail_region_kinds(&self) -> &'static [RegionKind] {
		use crate::exec::ir::logical::verify::{LIVE_TAIL, PLACE_TAIL, stage_tail};
		match self {
			PhysOp::WriteStage {
				stage,
				..
			} => stage_tail(*stage),
			PhysOp::Assign(..)
			| PhysOp::Unset {
				..
			} => PLACE_TAIL,
			PhysOp::Live(..) => LIVE_TAIL,
			// The predicate judged once, when the filter holds one.
			PhysOp::Filter => &[RegionKind::Clause(ClauseRole::Where)],
			PhysOp::Yield
			| PhysOp::Return
			| PhysOp::Break
			| PhysOp::Next
			| PhysOp::TableScan(..)
			| PhysOp::IndexScan(..)
			| PhysOp::IndexUnion(..)
			| PhysOp::Bitmap(..)
			| PhysOp::IndexLookup(..)
			| PhysOp::FullTextScan(..)
			| PhysOp::KeyRangeScan(..)
			| PhysOp::KnnScan(..)
			| PhysOp::KnnTopK(..)
			| PhysOp::Count(..)
			| PhysOp::RecordScan(..)
			| PhysOp::Vacant {
				..
			}
			| PhysOp::Enforced {
				..
			}
			| PhysOp::Values {
				..
			}
			| PhysOp::Subquery
			| PhysOp::Probe {
				..
			}
			| PhysOp::Union
			| PhysOp::Join(..)
			| PhysOp::Distinct {
				..
			}
			| PhysOp::PermissionFilter(..)
			| PhysOp::Layer(..)
			| PhysOp::Fetch
			| PhysOp::Split {
				..
			}
			| PhysOp::Sort(..)
			| PhysOp::Window(..)
			| PhysOp::Aggregate(..)
			| PhysOp::Limit {
				..
			}
			| PhysOp::Project(..)
			| PhysOp::Eval
			| PhysOp::Timeout
			| PhysOp::VersionScope
			| PhysOp::Only
			| PhysOp::Explain {
				..
			}
			| PhysOp::Content {
				..
			}
			| PhysOp::Request {
				..
			}
			| PhysOp::Txn {
				..
			}
			| PhysOp::Stmt {
				..
			}
			| PhysOp::Statement {
				..
			}
			| PhysOp::Match {
				..
			}
			| PhysOp::MatchStep(..)
			| PhysOp::DdlGuard {
				..
			}
			| PhysOp::Define(..)
			| PhysOp::Remove(..)
			| PhysOp::Alter(..)
			| PhysOp::Rebuild(..)
			| PhysOp::Info {
				..
			}
			| PhysOp::Use {
				..
			}
			| PhysOp::Option {
				..
			}
			| PhysOp::Access(..)
			| PhysOp::Kill
			| PhysOp::Subscriber(..)
			| PhysOp::Show(..)
			| PhysOp::Write(..)
			| PhysOp::Events(..)
			| PhysOp::Block
			| PhysOp::Let(..)
			| PhysOp::Sleep
			| PhysOp::If {
				..
			}
			| PhysOp::Loop {
				..
			}
			| PhysOp::Pipeline(..)
			| PhysOp::Choose(..)
			| PhysOp::Prefetch(..)
			| PhysOp::Materialize {
				..
			}
			| PhysOp::LoadUnit(..) => &[],
		}
	}

	/// The position in [`PhysOp::region_kinds`] of the entry that stands for any number of regions
	/// of its kind, none included: the clauses of a split, a fetch, and an access statement's
	/// filter, whose count the attributes do not hold.
	pub(crate) fn open_region_position(&self) -> Option<usize> {
		match self {
			PhysOp::Split {
				..
			}
			| PhysOp::Fetch => Some(0),
			PhysOp::Access(attrs) => match attrs.op {
				AccessOp::Show {
					..
				}
				| AccessOp::Revoke {
					..
				} => Some(0),
				AccessOp::Grant {
					..
				}
				| AccessOp::Purge {
					..
				} => None,
			},
			PhysOp::Yield
			| PhysOp::Return
			| PhysOp::Break
			| PhysOp::Next
			| PhysOp::TableScan(..)
			| PhysOp::IndexScan(..)
			| PhysOp::IndexUnion(..)
			| PhysOp::Bitmap(..)
			| PhysOp::IndexLookup(..)
			| PhysOp::FullTextScan(..)
			| PhysOp::KeyRangeScan(..)
			| PhysOp::KnnScan(..)
			| PhysOp::KnnTopK(..)
			| PhysOp::Count(..)
			| PhysOp::RecordScan(..)
			| PhysOp::Vacant {
				..
			}
			| PhysOp::Enforced {
				..
			}
			| PhysOp::Values {
				..
			}
			| PhysOp::Subquery
			| PhysOp::Probe {
				..
			}
			| PhysOp::Union
			| PhysOp::Join(..)
			| PhysOp::Distinct {
				..
			}
			| PhysOp::PermissionFilter(..)
			| PhysOp::Layer(..)
			| PhysOp::Filter
			| PhysOp::Sort(..)
			| PhysOp::Window(..)
			| PhysOp::Aggregate(..)
			| PhysOp::Limit {
				..
			}
			| PhysOp::Project(..)
			| PhysOp::Eval
			| PhysOp::Timeout
			| PhysOp::VersionScope
			| PhysOp::Only
			| PhysOp::Explain {
				..
			}
			| PhysOp::Assign(..)
			| PhysOp::Unset {
				..
			}
			| PhysOp::Content {
				..
			}
			| PhysOp::Request {
				..
			}
			| PhysOp::Txn {
				..
			}
			| PhysOp::Stmt {
				..
			}
			| PhysOp::Statement {
				..
			}
			| PhysOp::Match {
				..
			}
			| PhysOp::MatchStep(..)
			| PhysOp::DdlGuard {
				..
			}
			| PhysOp::Define(..)
			| PhysOp::Remove(..)
			| PhysOp::Alter(..)
			| PhysOp::Rebuild(..)
			| PhysOp::Info {
				..
			}
			| PhysOp::Use {
				..
			}
			| PhysOp::Option {
				..
			}
			| PhysOp::Kill
			| PhysOp::Live(..)
			| PhysOp::Subscriber(..)
			| PhysOp::Show(..)
			| PhysOp::Write(..)
			| PhysOp::WriteStage {
				..
			}
			| PhysOp::Events(..)
			| PhysOp::Block
			| PhysOp::Let(..)
			| PhysOp::Sleep
			| PhysOp::If {
				..
			}
			| PhysOp::Loop {
				..
			}
			| PhysOp::Pipeline(..)
			| PhysOp::Choose(..)
			| PhysOp::Prefetch(..)
			| PhysOp::Materialize {
				..
			}
			| PhysOp::LoadUnit(..) => None,
		}
	}

	/// The kind of each of the op's regions, in position order.
	pub(crate) fn region_kinds(&self) -> RegionKinds {
		match self {
			PhysOp::KnnScan(attrs) => {
				let mut kinds = SmallVec::new();
				if let Some(filter) = &attrs.filter {
					for permission in &filter.permissions {
						kinds.push(RegionKind::PermissionFrame);
						kinds.extend(std::iter::repeat_n(
							RegionKind::FieldFrame(FieldRole::Computed),
							permission.regions() - 1,
						));
					}
					if filter.residual {
						kinds.push(RegionKind::Clause(ClauseRole::Where));
					}
					if filter.matches_condition {
						kinds.push(RegionKind::Clause(ClauseRole::Where));
					}
					if filter.prefilter.as_ref().is_some_and(|prefilter| prefilter.residual) {
						kinds.push(RegionKind::Clause(ClauseRole::Where));
					}
				}
				kinds
			}
			PhysOp::Bitmap(attrs) => {
				if attrs.fallback {
					smallvec![RegionKind::Pipeline]
				} else {
					SmallVec::new()
				}
			}
			PhysOp::Count(attrs) => {
				let mut kinds: RegionKinds = match &attrs.shape {
					CountShape::Indexed {
						..
					} => smallvec![RegionKind::Clause(ClauseRole::Where)],
					CountShape::All {
						..
					} => SmallVec::new(),
				};
				// A count under a conditional permission holds the statement's ordinary plan
				// last, which runs when the caller's reads are judged.
				if attrs.select == crate::exec::vm::payload::CountSelect::Conditional {
					kinds.push(RegionKind::Subquery);
				}
				kinds
			}
			PhysOp::Subquery => smallvec![RegionKind::Subquery],
			PhysOp::Join(attrs) => {
				let mut kinds: RegionKinds = std::iter::repeat_n(
					RegionKind::Clause(ClauseRole::Where),
					usize::from(attrs.keys) * 2 + usize::from(attrs.residual),
				)
				.collect();
				if attrs.lookup.is_some() {
					kinds.push(RegionKind::JoinLookupArm);
				}
				kinds
			}
			PhysOp::PermissionFilter(attrs) => {
				let mut kinds: RegionKinds = smallvec![RegionKind::PermissionFrame];
				kinds.extend(std::iter::repeat_n(
					RegionKind::FieldFrame(FieldRole::Computed),
					attrs.computed.as_ref().map_or(0, ComputedReadsShape::derived_regions),
				));
				kinds
			}
			PhysOp::Layer(attrs) => {
				let count = field_regions(&attrs.fields);
				let mut kinds: RegionKinds =
					std::iter::repeat_n(RegionKind::Clause(ClauseRole::Projection), count)
						.collect();
				kinds.extend(std::iter::repeat_n(
					RegionKind::FieldFrame(FieldRole::Computed),
					count + attrs.judged.as_ref().map_or(0, ComputedReadsShape::derived_regions),
				));
				kinds
			}
			PhysOp::Filter => smallvec![RegionKind::Clause(ClauseRole::Where)],
			PhysOp::Fetch => smallvec![RegionKind::Clause(ClauseRole::Projection)],
			PhysOp::Split {
				..
			} => smallvec![RegionKind::Clause(ClauseRole::Split)],
			PhysOp::Sort(attrs) => std::iter::repeat_n(
				RegionKind::Clause(ClauseRole::OrderKey),
				attrs
					.keys
					.iter()
					.filter(|key| match key.read {
						SortKeyReadShape::Computed => true,
						SortKeyReadShape::Path(..) => false,
					})
					.count(),
			)
			.collect(),
			PhysOp::Window(shape) => {
				std::iter::repeat_n(RegionKind::Clause(ClauseRole::OrderKey), shape.key_regions())
					.chain(std::iter::repeat_n(
						RegionKind::Clause(ClauseRole::Projection),
						shape.arg_regions(),
					))
					.collect()
			}
			PhysOp::Aggregate(attrs) => {
				let mut kinds: RegionKinds = std::iter::repeat_n(
					RegionKind::ErrorScope(ErrorScope::ShapeToNone),
					usize::from(attrs.groups),
				)
				.collect();
				kinds.extend(std::iter::repeat_n(
					RegionKind::Clause(ClauseRole::Projection),
					field_regions(&attrs.fields),
				));
				kinds
			}
			PhysOp::Project(shape) => std::iter::repeat_n(
				RegionKind::Clause(ClauseRole::Projection),
				projection_regions(shape),
			)
			.collect(),
			PhysOp::Request {
				..
			} => smallvec![RegionKind::Request],
			PhysOp::Txn {
				mode,
			} => match mode {
				TxnRegionMode::Explicit {
					..
				} => smallvec![RegionKind::Transaction(TxnKind::Explicit)],
				TxnRegionMode::Session {
					..
				} => smallvec![RegionKind::Transaction(TxnKind::Session)],
			},
			PhysOp::Stmt {
				commit,
				..
			} => smallvec![RegionKind::Statement(*commit)],
			PhysOp::Statement {
				..
			} => smallvec![RegionKind::Statement(StatementCommit::InTxn)],
			PhysOp::Match {
				stages,
				..
			} => std::iter::repeat_n(RegionKind::Subquery, usize::from(*stages)).collect(),
			PhysOp::MatchStep(attrs) => match attrs.as_ref() {
				MatchStepAttrs::Read {
					predicates,
					..
				} => std::iter::repeat_n(
					RegionKind::Clause(ClauseRole::Where),
					usize::from(*predicates),
				)
				.collect(),
				MatchStepAttrs::Update(update) => match &update.data {
					MatchData::Set(names) => {
						std::iter::repeat_n(RegionKind::Clause(ClauseRole::SetValue), names.len())
							.collect()
					}
					MatchData::Unset(..) => SmallVec::new(),
					MatchData::Content => smallvec![RegionKind::Clause(ClauseRole::Content)],
				},
				MatchStepAttrs::Delete {
					..
				} => SmallVec::new(),
				MatchStepAttrs::Insert(insert) => std::iter::repeat_n(
					RegionKind::Clause(ClauseRole::Content),
					insert.nodes.len() + insert.edges.len(),
				)
				.collect(),
				MatchStepAttrs::Bind(shape) => shape.region_kinds(),
				MatchStepAttrs::Output(shape) => {
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
			},
			PhysOp::Define(attrs) => LogOp::Define(attrs.clone()).region_kinds(),
			PhysOp::Alter(attrs) => LogOp::Alter(attrs.clone()).region_kinds(),
			PhysOp::Access(attrs) => match attrs.op {
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
			PhysOp::Live(attrs) => match attrs.fields {
				LiveShape::Select => smallvec![RegionKind::StoredClause(StoredRole::LiveFields)],
				LiveShape::Diff => SmallVec::new(),
			},
			PhysOp::Subscriber(attrs) => attrs.region_kinds().into_iter().collect(),
			PhysOp::WriteStage {
				..
			} => smallvec![RegionKind::WriteStage],
			PhysOp::Events(attrs) => attrs.region_kinds().into_iter().collect(),
			PhysOp::Timeout => smallvec![RegionKind::Deadline],
			PhysOp::Write(attrs) => attrs.write.region_kinds().into_iter().collect(),
			PhysOp::Block => smallvec![RegionKind::Block],
			PhysOp::If {
				value,
			} => {
				let arm = if *value {
					RegionKind::ValueIf
				} else {
					RegionKind::StatementIf
				};
				smallvec![arm, arm]
			}
			PhysOp::Loop {
				kind,
				first,
				value,
				step,
				..
			} => {
				let body = match kind {
					LoopKind::For if *value => RegionKind::ValueFor,
					LoopKind::For => RegionKind::StatementFor,
					LoopKind::Select
					| LoopKind::Write
					| LoopKind::Closure
					| LoopKind::ElementFilter
					| LoopKind::GraphStep
					| LoopKind::Recurse
					| LoopKind::Broadcast => RegionKind::Loop,
				};
				let mut kinds: RegionKinds = smallvec![body];
				if *first {
					kinds.push(body);
				}
				if let Some(StepAttrs::Lookup(lookup)) = step.as_deref() {
					lookup_clause_kinds(lookup, &mut kinds);
				}
				kinds
			}
			PhysOp::Pipeline(..) => smallvec![RegionKind::Pipeline],
			PhysOp::Choose(spec) => {
				std::iter::repeat_n(RegionKind::ChooseArm, spec.arms.len()).collect()
			}
			PhysOp::TableScan(..)
			| PhysOp::IndexScan(..)
			| PhysOp::IndexUnion(..)
			| PhysOp::IndexLookup(..)
			| PhysOp::FullTextScan(..)
			| PhysOp::KeyRangeScan(..)
			| PhysOp::KnnTopK(..)
			| PhysOp::RecordScan(..)
			| PhysOp::Vacant {
				..
			}
			| PhysOp::Enforced {
				..
			}
			| PhysOp::Values {
				..
			}
			| PhysOp::Probe {
				..
			}
			| PhysOp::Union
			| PhysOp::Distinct {
				..
			}
			| PhysOp::Limit {
				..
			}
			| PhysOp::Eval
			| PhysOp::VersionScope
			| PhysOp::Only
			| PhysOp::Explain {
				..
			}
			| PhysOp::Assign(..)
			| PhysOp::Unset {
				..
			}
			| PhysOp::Content {
				..
			}
			| PhysOp::DdlGuard {
				..
			}
			| PhysOp::Remove(..)
			| PhysOp::Rebuild(..)
			| PhysOp::Info {
				..
			}
			| PhysOp::Use {
				..
			}
			| PhysOp::Option {
				..
			}
			| PhysOp::Kill
			| PhysOp::Show(..)
			| PhysOp::Let(..)
			| PhysOp::Sleep
			| PhysOp::Prefetch(..)
			| PhysOp::Materialize {
				..
			}
			| PhysOp::LoadUnit(..)
			| PhysOp::Yield
			| PhysOp::Return
			| PhysOp::Break
			| PhysOp::Next => SmallVec::new(),
		}
	}

	/// The operand and result counts the op accepts.
	///
	/// A statement or transaction op has one result, the statement's slot value; a relational op
	/// has one result, its rows.
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
			PhysOp::IndexUnion(..)
			| PhysOp::Bitmap(..)
			| PhysOp::FullTextScan(..)
			| PhysOp::Vacant {
				..
			}
			| PhysOp::Subquery
			| PhysOp::Request {
				..
			}
			| PhysOp::Txn {
				..
			}
			| PhysOp::Stmt {
				..
			}
			| PhysOp::Statement {
				..
			}
			| PhysOp::Match {
				..
			}
			| PhysOp::Option {
				..
			}
			| PhysOp::Show(..)
			| PhysOp::Block => exactly(0, 1),
			PhysOp::IndexScan(..) => at_least(0, 1),
			// A scan whose query vector is known only when the statement runs takes it as its
			// operand.
			PhysOp::KnnScan(attrs) => exactly(u16::from(attrs.vector.is_empty()), 1),
			PhysOp::KeyRangeScan(attrs) => exactly(attrs.access.holes() as u16, 1),
			PhysOp::TableScan(attrs) => {
				exactly(attrs.bounds.as_ref().map_or(0, |bounds| bounds.holes()) as u16, 1)
			}
			PhysOp::IndexLookup(..)
			| PhysOp::RecordScan(..)
			| PhysOp::Enforced {
				..
			}
			| PhysOp::Values {
				..
			}
			| PhysOp::Distinct {
				..
			}
			| PhysOp::PermissionFilter(..)
			| PhysOp::Layer(..)
			| PhysOp::Filter
			| PhysOp::Fetch
			| PhysOp::Split {
				..
			}
			| PhysOp::Sort(..)
			| PhysOp::Window(..)
			| PhysOp::Aggregate(..)
			| PhysOp::Project(..)
			| PhysOp::Eval
			| PhysOp::Only
			| PhysOp::Timeout
			| PhysOp::Explain {
				..
			}
			| PhysOp::Kill
			| PhysOp::Live(..)
			| PhysOp::Let(..)
			| PhysOp::Sleep
			| PhysOp::If {
				..
			}
			| PhysOp::Loop {
				step: None,
				..
			}
			| PhysOp::Materialize {
				..
			} => exactly(1, 1),
			PhysOp::Loop {
				step: Some(step),
				..
			} => {
				if step.takes_clauses() {
					at_least(1, 1)
				} else {
					exactly(1, 1)
				}
			}
			PhysOp::KnnTopK(attrs) => match attrs.vector {
				KnnTargetShape::Literal(..) => exactly(1, 1),
				KnnTargetShape::Deferred(..) => exactly(2, 1),
			},
			PhysOp::Count(attrs) => match &attrs.shape {
				CountShape::All {
					source,
					..
				} => match source {
					SourceRefShape::Table(..) => exactly(0, 1),
					SourceRefShape::RecordId
					| SourceRefShape::Mock
					| SourceRefShape::Param
					| SourceRefShape::Dynamic => exactly(1, 1),
				},
				CountShape::Indexed {
					..
				} => exactly(0, 1),
			},
			PhysOp::Probe {
				..
			}
			| PhysOp::Join(..) => exactly(2, 1),
			// A union of no inputs is the empty scan.
			PhysOp::Union => at_least(0, 1),
			PhysOp::Limit {
				..
			} => at_least(1, 1),
			PhysOp::MatchStep(attrs) => match attrs.as_ref() {
				MatchStepAttrs::Read {
					..
				}
				| MatchStepAttrs::Update(..)
				| MatchStepAttrs::Delete {
					..
				}
				| MatchStepAttrs::Insert(..) => exactly(1, 1),
				MatchStepAttrs::Output(..) => at_least(1, 1),
				MatchStepAttrs::Bind(shape) => exactly(shape.operands(), 1),
			},
			PhysOp::DdlGuard {
				..
			} => at_least(0, 0),
			PhysOp::Define(..) | PhysOp::Remove(..) | PhysOp::Alter(..) | PhysOp::Rebuild(..) => {
				at_least(0, 1)
			}
			PhysOp::Info {
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
			PhysOp::Use {
				target,
			} => match target {
				UseTarget::Ns | UseTarget::Db => exactly(1, 1),
				UseTarget::NsDb => exactly(2, 1),
				UseTarget::Default => exactly(0, 1),
			},
			PhysOp::Access(attrs) => match &attrs.op {
				AccessOp::Grant {
					subject,
				} => match subject {
					AccessSubjectShape::Record => at_least(1, 1),
					AccessSubjectShape::User(..) => at_least(0, 1),
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
			PhysOp::Write(attrs) => match attrs.subjects {
				WriteSubjectsShape::Rows => exactly(1, 1),
				// The three endpoints of a `RELATE`, or the one result of the loop that delivers
				// them.
				WriteSubjectsShape::Edges
				| WriteSubjectsShape::Targets
				| WriteSubjectsShape::Guaranteed
				| WriteSubjectsShape::Inserts => at_least(1, 1),
			},
			PhysOp::VersionScope => exactly(2, 1),
			PhysOp::Assign(..) => at_least(1, 0),
			PhysOp::Unset {
				..
			} => at_least(0, 0),
			PhysOp::Content {
				..
			} => exactly(1, 0),
			PhysOp::WriteStage {
				..
			}
			| PhysOp::Events(..)
			| PhysOp::Subscriber(..)
			| PhysOp::Break
			| PhysOp::Next => exactly(0, 0),
			PhysOp::Pipeline(..) | PhysOp::LoadUnit(..) => at_least(0, 1),
			PhysOp::Choose(..) => at_least(0, 1),
			PhysOp::Prefetch(..) | PhysOp::Yield => at_least(0, 0),
			PhysOp::Return => exactly(1, 0),
		}
	}

	/// How the op reaches the interpreter on the target `isa` describes.
	///
	/// `Native` names the ops whose instructions the leaf profile classifies as
	/// [`LeafClass::Leaf`]: the interpreter alone runs them. An op that reaches storage, the
	/// catalog or the principal through the host, a write and a statement-family op are a call of
	/// a unit.
	pub(crate) fn lowers_natively(&self, _isa: IsaCaps) -> NativeLowering {
		match self {
			PhysOp::Subquery
			| PhysOp::Probe {
				..
			}
			| PhysOp::Union
			| PhysOp::Join(..)
			| PhysOp::Sort(..)
			| PhysOp::Window(..)
			| PhysOp::KnnTopK(..)
			| PhysOp::Aggregate(..)
			| PhysOp::Split {
				..
			}
			| PhysOp::Filter
			| PhysOp::Limit {
				..
			} => NativeLowering::Native {
				leaf_class: LeafClass::Leaf,
			},
			PhysOp::TableScan(..)
			| PhysOp::IndexScan(..)
			| PhysOp::IndexUnion(..)
			| PhysOp::Bitmap(..)
			| PhysOp::IndexLookup(..)
			| PhysOp::FullTextScan(..)
			| PhysOp::KeyRangeScan(..)
			| PhysOp::KnnScan(..)
			| PhysOp::Count(..)
			| PhysOp::RecordScan(..)
			| PhysOp::Vacant {
				..
			}
			| PhysOp::Enforced {
				..
			}
			| PhysOp::Values {
				..
			}
			| PhysOp::Distinct {
				..
			}
			| PhysOp::PermissionFilter(..)
			| PhysOp::Layer(..)
			| PhysOp::Fetch
			| PhysOp::Project(..)
			| PhysOp::Eval
			| PhysOp::Timeout
			| PhysOp::VersionScope
			| PhysOp::Only
			| PhysOp::Explain {
				..
			}
			| PhysOp::Assign(..)
			| PhysOp::Unset {
				..
			}
			| PhysOp::Content {
				..
			}
			| PhysOp::Request {
				..
			}
			| PhysOp::Txn {
				..
			}
			| PhysOp::Stmt {
				..
			}
			| PhysOp::Statement {
				..
			}
			| PhysOp::Match {
				..
			}
			| PhysOp::MatchStep(..)
			| PhysOp::DdlGuard {
				..
			}
			| PhysOp::Define(..)
			| PhysOp::Remove(..)
			| PhysOp::Alter(..)
			| PhysOp::Rebuild(..)
			| PhysOp::Info {
				..
			}
			| PhysOp::Use {
				..
			}
			| PhysOp::Option {
				..
			}
			| PhysOp::Access(..)
			| PhysOp::Kill
			| PhysOp::Live(..)
			| PhysOp::Subscriber(..)
			| PhysOp::Show(..)
			| PhysOp::Write(..)
			| PhysOp::WriteStage {
				..
			}
			| PhysOp::Events(..)
			| PhysOp::Block
			| PhysOp::Let(..)
			| PhysOp::Sleep
			| PhysOp::If {
				..
			}
			| PhysOp::Loop {
				..
			}
			| PhysOp::Pipeline(..)
			| PhysOp::Choose(..)
			| PhysOp::Prefetch(..)
			| PhysOp::Materialize {
				..
			}
			| PhysOp::LoadUnit(..)
			| PhysOp::Yield
			| PhysOp::Return
			| PhysOp::Break
			| PhysOp::Next => NativeLowering::UnitCall,
		}
	}
}

/// The number of `Clause(Projection)` regions a projection list holds: one per field that is not
/// `*`.
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

/// The number of `Clause(Projection)` regions a projection holds: one per expression its shape
/// lists, the omissions' expressions after the fields.
fn projection_regions(shape: &ProjectionShape) -> usize {
	match shape {
		ProjectionShape::All | ProjectionShape::SortedValue(..) => 0,
		ProjectionShape::Value => 1,
		ProjectionShape::OmittedValue {
			omit,
		} => 1 + omission_regions(omit),
		ProjectionShape::Fields {
			fields,
			omit,
		}
		| ProjectionShape::Star {
			fields,
			omit,
		} => fields.len() + omission_regions(omit),
	}
}

/// The number of regions the omitted paths hold: a walk holds its source and each step or action
/// that holds an expression.
fn omission_regions(omit: &[OmissionShape]) -> usize {
	omit.iter()
		.map(|omission| match omission {
			OmissionShape::Path(..) => 0,
			OmissionShape::Walk {
				steps,
				action,
			} => {
				let in_steps: usize = steps
					.iter()
					.map(|step| match step {
						WalkStepShape::IndexAt | WalkStepShape::Matching => 1,
						WalkStepShape::Destructure(entries) => destructure_regions(entries),
						WalkStepShape::Field(..)
						| WalkStepShape::Index(..)
						| WalkStepShape::Each
						| WalkStepShape::First
						| WalkStepShape::Last
						| WalkStepShape::Nowhere => 0,
					})
					.sum();
				let in_action = match action {
					WalkActionShape::RemoveIndexAt | WalkActionShape::RemoveMatching => 1,
					WalkActionShape::Destructure(entries) => destructure_regions(entries),
					WalkActionShape::RemoveField(..)
					| WalkActionShape::RemoveIndex(..)
					| WalkActionShape::RemoveFirst
					| WalkActionShape::RemoveLast
					| WalkActionShape::Clear
					| WalkActionShape::Nothing => 0,
				};
				1 + in_steps + in_action
			}
		})
		.sum()
}

/// The number of regions a destructure holds: one per aliased entry, nested entries included.
fn destructure_regions(entries: &[DestructureEntry]) -> usize {
	entries
		.iter()
		.map(|entry| match entry {
			DestructureEntry::Aliased(..) => 1,
			DestructureEntry::Destructure(_, nested) => destructure_regions(nested),
			DestructureEntry::All(..) | DestructureEntry::Field(..) | DestructureEntry::Rest => 0,
		})
		.sum()
}

/// What the physical phase answers for one container.
#[derive(Clone, Debug)]
pub(crate) struct PhysicalOutcome {
	/// What the conversion did: the ops it converted and eliminated and the limit it stopped at.
	pub(crate) report: ConversionReport,
	/// The plan-time values the conversion's decisions consumed, one dependence per decision and
	/// value.
	pub(crate) deps: ValueDeps,
	/// Whether `deps` names every value the decisions consumed. A literal and a name that resolves
	/// through the request's bindings or the session are not named, so a record that is not
	/// complete lists only part of what the decisions depend on.
	pub(crate) deps_complete: bool,
}

/// Plans `ir` physically: converts it to the physical dialect, builds its pipelines, derives its
/// pipeline graph and decides every pipeline's placement.
///
/// In order: the count fast path of the statements it answers ([`count::offer`]), the limit of a
/// statement read only for existence ([`existence::offer`]) and the index seek of an extreme
/// ([`extreme::offer`]), which read the logical shapes the conversion consumes; the conversion with
/// the registered patterns, folds
/// awaited through `folder`, which creates no pipeline; the dependences of the decisions that
/// consumed a folded value, read from `facts` once the conversion is done; the definitions of the
/// user-defined functions the container calls, stated on each call ([`callee::read_callees`]); the
/// searches that answer the statements' `KNN` operators ([`knn::run`]); the pipelines, one around
/// each fan-out loop; the pipeline graph, checked; the placement of each pipeline in graph order on
/// the backend the physical catalog states, through `placement`; and the placements' installation
/// in the container's placement table. The caller verifies the container at the physical phase
/// boundary.
///
/// The strategies of statements in value position are decided after this phase. Until then the
/// placement reads each one that only reads as evaluated once or through a probe
/// ([`facts::NestedReading::Hedged`]). [`rewrite::run`] places the container again once those
/// strategies are decided, so the placement reflects them.
///
/// The container holds no effect token: the caller has stripped them
/// ([`tokens::strip`](crate::exec::ir::analysis::tokens::strip)), the physical dialect holds none,
/// and the logical ops the edits before the conversion make take none. The existence and extreme
/// edits refuse a container that does.
///
/// The caller fills every table the container reads before planning. A source whose answers
/// are not loaded is an invariant break: the conversion, and the stored query vector of a
/// brute-force search, report [`IrError::NotLoaded`] for it.
/// That error is answered as [`IrError::Invariant`].
pub(crate) async fn plan_physical(
	ir: &mut Ir,
	facts: &mut FactCache,
	cx: &PhysCx<'_>,
	folder: &dyn PlanTimeFolder,
	placement: &placement::Placement,
) -> Result<PhysicalOutcome, IrError> {
	count::offer(ir, facts, cx)?;
	existence::offer(ir, facts, cx)?;
	extreme::offer(ir, facts, cx)?;
	matches::run(ir, cx.effects.registry);
	knn::hoist_vectors(ir, facts)?;
	let report = convert(ir, facts, &registry::build().conversion, cx, folder, &PHYSICAL_PHASE)
		.await
		.map_err(|error| {
			if let IrError::NotLoaded {
				what,
			} = error
			{
				IrError::Invariant {
					what,
				}
			} else {
				error
			}
		})?;
	let (deps, deps_complete) = conversion::value_deps(ir, facts);
	callee::read_callees(ir, facts, cx.effects.catalog, cx.effects.registry)?;
	knn::run(ir, facts, cx).map_err(|error| {
		if let IrError::NotLoaded {
			what,
		} = error
		{
			IrError::Invariant {
				what,
			}
		} else {
			error
		}
	})?;
	write_prune::run(ir, facts, cx)?;
	let top = ir.top();
	pipeline::build(ir, facts, top)?;
	pipeline::assign(ir, facts)?;
	let placed =
		placement::place_all(ir, facts, cx.effects.registry, placement, cx.physical.backend())?;
	placement::install(ir.placement_table_mut(), placed);
	Ok(PhysicalOutcome {
		report,
		deps,
		deps_complete,
	})
}

#[cfg(test)]
mod tests {
	use super::ops::{AggregateAttrs, PhysWriteAttrs};
	use super::*;
	use crate::exec::ir::iface::statements::{MatchBindings, OutputShape, WriteFlags, WriteForm};
	use crate::exec::ir::logical::ops::WriteAttrs;
	use crate::exec::vm::payload::Spill;
	use crate::expr::ExplainFormat;

	fn aggregate(groups: u16) -> PhysOp {
		PhysOp::Aggregate(Box::new(AggregateAttrs {
			groups,
			names: Vec::new(),
			fields: FieldsShape::Value {
				alias: None,
			},
			first_value: Vec::new(),
			spill: Spill::Spillable,
		}))
	}

	fn write(output: OutputShape) -> PhysOp {
		PhysOp::Write(Box::new(PhysWriteAttrs {
			write: WriteAttrs {
				form: WriteForm::Create,
				only: false,
				output,
				with: None,
				explain: None,
				flags: WriteFlags::default(),
				body: Default::default(),
				gates: Default::default(),
				image: Default::default(),
				stored: Default::default(),
				derived: Default::default(),
				into_text: None,
				statement: None,
				linked: None,
			},
			subjects: WriteSubjectsShape::Targets,
		}))
	}

	#[test]
	fn an_if_and_a_for_state_their_position() {
		for (value, arm, body) in [
			(false, RegionKind::StatementIf, RegionKind::StatementFor),
			(true, RegionKind::ValueIf, RegionKind::ValueFor),
		] {
			assert_eq!(
				PhysOp::If {
					value
				}
				.region_kinds()
				.as_slice(),
				&[arm, arm]
			);
			let looped = |first| PhysOp::Loop {
				kind: LoopKind::For,
				first,
				batch: super::ops::Batch {
					max_iterations: 1,
					max_bytes: 1,
				},
				unroll: 1,
				value,
				step: None,
			};
			assert_eq!(looped(false).region_kinds().as_slice(), &[body]);
			assert_eq!(looped(true).region_kinds().as_slice(), &[body, body]);
		}
	}

	#[test]
	fn a_match_and_its_read_stage_state_their_counts() {
		let query = PhysOp::Match {
			bindings: MatchBindings(Box::new([])),
			stages: 3,
		};
		assert_eq!(query.region_kinds().as_slice(), &[RegionKind::Subquery; 3]);
		let read = PhysOp::MatchStep(Box::new(MatchStepAttrs::Read {
			optional_group: None,
			patterns: Box::new([]),
			predicates: 2,
		}));
		assert_eq!(read.region_kinds().as_slice(), &[RegionKind::Clause(ClauseRole::Where); 2]);
	}

	#[test]
	fn an_aggregation_states_a_region_per_group_key() {
		let key = RegionKind::ErrorScope(ErrorScope::ShapeToNone);
		let field = RegionKind::Clause(ClauseRole::Projection);
		assert_eq!(aggregate(0).region_kinds().as_slice(), &[field]);
		assert_eq!(aggregate(2).region_kinds().as_slice(), &[key, key, field]);
	}

	#[test]
	fn a_write_holds_a_return_region_only_for_fields() {
		assert_eq!(
			write(OutputShape::Fields).region_kinds().as_slice(),
			&[RegionKind::Clause(ClauseRole::ReturnClause)]
		);
		for output in [
			OutputShape::None,
			OutputShape::Null,
			OutputShape::Diff,
			OutputShape::After,
			OutputShape::Before,
		] {
			assert!(write(output).region_kinds().is_empty());
		}
	}

	#[test]
	fn the_clauses_of_a_statement_state_their_shape() {
		let exactly = |operands, results| Arity {
			operands: OperandRule::Exactly(operands),
			results,
		};
		assert_eq!(PhysOp::Timeout.region_kinds().as_slice(), &[RegionKind::Deadline]);
		assert_eq!(PhysOp::Timeout.arity(), exactly(1, 1));
		assert_eq!(PhysOp::VersionScope.arity(), exactly(2, 1));
		assert_eq!(PhysOp::Only.arity(), exactly(1, 1));
		let explain = PhysOp::Explain {
			analyze: false,
			format: ExplainFormat::Text,
		};
		assert_eq!(explain.arity(), exactly(1, 1));
		let assign = PhysOp::Assign(Box::new(crate::exec::ir::logical::ops::AssignAttrs {
			place: crate::exec::ir::iface::statements::Place {
				parts: Box::new([]),
			},
			op: crate::expr::AssignOperator::Assign,
		}));
		assert_eq!(
			assign.arity(),
			Arity {
				operands: OperandRule::AtLeast(1),
				results: 0,
			}
		);
		let content = PhysOp::Content {
			mode: crate::exec::ir::iface::statements::ContentMode::Content,
		};
		assert_eq!(content.arity(), exactly(1, 0));
		for op in [PhysOp::Only, PhysOp::VersionScope, explain, assign, content] {
			assert!(op.region_kinds().is_empty(), "{}", op.tag().mnemonic());
			assert!(!op.is_terminator());
		}
	}
}
