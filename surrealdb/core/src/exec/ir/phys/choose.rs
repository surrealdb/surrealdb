//! Construction of physical `choose` ops: guarded alternatives decided at run time.
//!
//! A physical choice holds at least two arms and the generic arm every input admits. Its guards
//! are total: a guard that cannot be evaluated (a size bound over a closed transaction, or over a
//! backend that gives no estimate) selects the generic arm, whose answer equals every other
//! arm's. Switching arms compiles nothing during the run. Arms over different access paths are
//! built only under total `ORDER BY` keys, so ties never make two arms differ.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;

use super::ops::{PhysOp, ProjectionShape, SortAttrs, SortKeyReadShape};
use super::order;
use super::pipeline::enclosing_region_of;
use crate::exec::field_path::FieldPath;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::iface::choose::{
	ArmBody, ArmEquivalence, ArmSpec, ChooseGranularity, ChooseOrigin, ChooseSpec, GenericArm,
	Guard, MAX_CHOOSE_ARMS,
};
use crate::exec::ir::iface::ids::{OpId, RegionId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::planner::placement::PhysicalFacts;

/// The ops of one statement's own body that decide whether a sort key ending in `id` is total.
#[derive(Default)]
struct ScopeScan<'a> {
	/// Every sort of the body.
	sorts: Vec<&'a SortAttrs>,
	/// Whether an op of the body can give several rows one record id, or lets a projection name
	/// another value `id`.
	repeats_ids: bool,
}

impl<'a> ScopeScan<'a> {
	/// Scans the ops of `region` and of the regions beneath them, never entering a nested
	/// statement or subquery, whose sorts and rows are their own.
	fn region(&mut self, ir: &'a Ir, region: RegionId) {
		for op in ops_in(ir, region) {
			let Some(data) = ir.op(op) else {
				continue;
			};
			if let OpKind::Phys(kind) = &data.kind {
				self.op(kind);
			}
			for inner in &data.regions {
				let own = ir.region(*inner).is_some_and(|data| {
					!matches!(data.kind, RegionKind::Statement(_) | RegionKind::Subquery)
				});
				if own {
					self.region(ir, *inner);
				}
			}
		}
	}

	fn op(&mut self, op: &'a PhysOp) {
		match op {
			PhysOp::Sort(attrs) => self.sorts.push(attrs),
			// Rows are grouped, expanded, paired or concatenated: one record id may head several.
			PhysOp::Aggregate(..)
			| PhysOp::Split {
				..
			}
			| PhysOp::Join(..)
			| PhysOp::Union => self.repeats_ids = true,
			PhysOp::Project(shape) => self.repeats_ids |= renames_to_id(shape),
			// Every other op gives each record id at most once. An op that can repeat an id must be
			// listed in the arm above.
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
			| PhysOp::Distinct {
				..
			}
			| PhysOp::Window(..)
			| PhysOp::PermissionFilter(..)
			| PhysOp::Layer(..)
			| PhysOp::Filter
			| PhysOp::Fetch
			| PhysOp::Limit {
				..
			}
			| PhysOp::Timeout
			| PhysOp::VersionScope
			| PhysOp::Only
			| PhysOp::Explain {
				..
			}
			| PhysOp::Eval
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
			| PhysOp::Assign(..)
			| PhysOp::Unset {
				..
			}
			| PhysOp::Content {
				..
			}
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
			| PhysOp::Next => {}
		}
	}
}

/// Whether a projection lands an aliased field under the name `id`.
fn renames_to_id(shape: &ProjectionShape) -> bool {
	let id = FieldPath::field("id");
	match shape {
		ProjectionShape::Fields {
			fields,
			..
		}
		| ProjectionShape::Star {
			fields,
			..
		} => fields.iter().any(|field| field.aliased && field.output == id),
		ProjectionShape::All
		| ProjectionShape::Value
		| ProjectionShape::OmittedValue {
			..
		}
		| ProjectionShape::SortedValue(..) => false,
	}
}

/// Whether the keys of `sort` end in the record id, so that no two rows tie: the last key is total
/// by [`order::key_is_total`] and the order is not random.
fn keys_end_in_id(sort: &SortAttrs) -> bool {
	!sort.random && sort.keys.last().is_some_and(|key| {
		let reads_id =
			matches!(&key.read, SortKeyReadShape::Path(path) if *path == FieldPath::field("id"));
		order::key_is_total(key.collate, key.numeric, reads_id)
	})
}

/// Whether the body of a statement orders its rows by keys that never tie: it sorts, every sort
/// ends in the record id, and no op of the body gives several rows one id.
fn scope_keys_total(scan: &ScopeScan<'_>) -> bool {
	!scan.repeats_ids
		&& !scan.sorts.is_empty()
		&& scan.sorts.iter().all(|sort| keys_end_in_id(sort))
}

/// The witness that the `ORDER BY` keys of a statement are total ([`sort_keys_total`]), which
/// nothing else constructs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TotalKeys(());

/// The access paths the arms of a choice read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArmPaths {
	/// Every arm reads the same access path.
	Same,
	/// The arms read different access paths, under sort keys that never tie.
	Differ(TotalKeys),
}

/// The witness that the `ORDER BY` keys of the statement that holds `op` are total: its rows come
/// out in one sequence whatever access path produced them.
///
/// The statement is the nearest `Statement` or `Subquery` region around `op`; the nested ones
/// inside it are not searched. The keys are total when the statement sorts, every `phys.sort` of
/// it ends in the record id, and nothing in it groups, splits, joins or concatenates rows or
/// renames a field to `id`. The keys of a unique index count only once the physical catalog states
/// them, so the answer may refuse a choice that is safe and never admits one that is not. A
/// statement with no sort has no total keys.
pub(crate) fn sort_keys_total(ir: &Ir, op: OpId) -> Option<TotalKeys> {
	let (_, region) = enclosing_region_of(ir, op, |kind| {
		matches!(kind, RegionKind::Statement(_) | RegionKind::Subquery)
	})?;
	let mut scan = ScopeScan::default();
	scan.region(ir, region);
	scope_keys_total(&scan).then_some(TotalKeys(()))
}

/// The choice between the prepared `arms` of a physical choice, or `None` when the statement keeps
/// one fixed plan.
///
/// Arms over different access paths hold the witness that the statement's sort keys are total
/// ([`ArmPaths::Differ`]): the sort keeps ties in arrival order, so under keys that tie the arms
/// would return different rows. Nothing is built for fewer than two arms, more than
/// [`MAX_CHOOSE_ARMS`], an arm that is not lowered with the choice, or a last guard that is not
/// [`Guard::Otherwise`], because the verifier rejects each. The choice switches once per
/// statement; its arms are equivalent by total keys when their access paths differ, and by
/// returning the same rows in the same order otherwise.
pub(crate) fn physical_choice(arms: SmallVec<[ArmSpec; 4]>, paths: ArmPaths) -> Option<ChooseSpec> {
	if arms.len() < 2 || arms.len() > MAX_CHOOSE_ARMS {
		return None;
	}
	if !matches!(arms.last(), Some(arm) if arm.guard == Guard::Otherwise) {
		return None;
	}
	if arms.iter().any(|arm| arm.body != ArmBody::PreLowered) {
		return None;
	}
	Some(ChooseSpec {
		granularity: ChooseGranularity::Statement,
		arms,
		generic: GenericArm::Present,
		equivalence: match paths {
			ArmPaths::Differ(TotalKeys(())) => ArmEquivalence::OrderedByTotalKey,
			ArmPaths::Same => ArmEquivalence::SameRowsSameOrder,
		},
		origin: ChooseOrigin::PhysicalGuarded,
	})
}

/// The choice between the two sides of a join, or `None` when the join keeps one fixed rule.
///
/// The hash, single-row and lookup joins pair rows in different orders, so arms over them are not
/// equivalent. No plan-time estimate of either side is available, so this returns `None` for every
/// input. A join choice needs both a size estimate and total sort keys in its statement.
pub(crate) fn join_choice(_left: &PhysicalFacts, _right: &PhysicalFacts) -> Option<ChooseSpec> {
	None
}

/// The guard that checks a type assumption at run time: [`Guard::TypeHolds`] for a hint, `None`
/// for a fact, which needs no check, and for unknown knowledge, which supports no assumption.
pub(crate) fn type_guard(knowledge: &TypeKnowledge) -> Option<Guard> {
	match knowledge {
		TypeKnowledge::Hint {
			kind,
			..
		} => Some(Guard::TypeHolds(kind.clone())),
		TypeKnowledge::Fact {
			..
		}
		| TypeKnowledge::Token(_)
		| TypeKnowledge::Unknown => None,
	}
}

#[cfg(test)]
mod tests {
	use smallvec::smallvec;
	use surrealdb_strand::{Strand, TableName};

	use super::*;
	use crate::exec::ir::iface::choose::{ArmLabel, IndexRef};
	use crate::exec::ir::iface::estimate::SizeBucket;
	use crate::exec::ir::iface::types::{FactSource, HintSource};
	use crate::exec::ir::phys::ops::{ProjectedFieldShape, SortKeySpecShape};
	use crate::exec::planner::placement::{NestedUse, SinkKind, SourceKind};
	use crate::exec::vm::payload::Spill;
	use crate::expr::Kind;

	fn table(name: &str) -> TableName {
		TableName::new(Strand::new(name))
	}

	fn index(table_name: &str) -> IndexRef {
		IndexRef {
			table: table(table_name),
			index: Strand::new("ix"),
			position: 0,
			scalar: Box::default(),
		}
	}

	fn arm(guard: Guard) -> ArmSpec {
		ArmSpec {
			guard,
			body: ArmBody::PreLowered,
			label: ArmLabel("arm"),
		}
	}

	fn key(field: &str) -> SortKeySpecShape {
		SortKeySpecShape {
			read: SortKeyReadShape::Path(FieldPath::field(field)),
			ascending: true,
			collate: false,
			numeric: false,
		}
	}

	fn sort(keys: Vec<SortKeySpecShape>) -> SortAttrs {
		SortAttrs {
			keys,
			keep: None,
			pushdown: false,
			random: false,
			spill: Spill::Spillable,
			tempfiles: false,
		}
	}

	fn two_arms() -> SmallVec<[ArmSpec; 4]> {
		smallvec![
			arm(Guard::SizeAtMost {
				bucket: SizeBucket::of_rows(1000)
			}),
			arm(Guard::Otherwise)
		]
	}

	fn facts() -> PhysicalFacts {
		PhysicalFacts {
			source: SourceKind::Scan,
			enforce: false,
			writes: false,
			limits: 0,
			output: true,
			sink: SinkKind::Output,
			binds_row: false,
			fetches_row: false,
			distincts: 0,
			nested: NestedUse::None,
			host_blocks: false,
			paging_replayable: false,
			estimate: None,
		}
	}

	#[test]
	fn tied_keys_build_no_choice() {
		assert!(!scope_keys_total(&ScopeScan::default()));
		let single_path = physical_choice(two_arms(), ArmPaths::Same).expect("one access path");
		assert_eq!(single_path.equivalence, ArmEquivalence::SameRowsSameOrder);
	}

	#[test]
	fn keys_ending_in_id_may_choose() {
		let spec = physical_choice(two_arms(), ArmPaths::Differ(TotalKeys(())))
			.expect("the keys are total");
		assert_eq!(spec.equivalence, ArmEquivalence::OrderedByTotalKey);
		assert_eq!(spec.origin, ChooseOrigin::PhysicalGuarded);
		assert_eq!(spec.granularity, ChooseGranularity::Statement);
		assert!(keys_end_in_id(&sort(vec![key("y"), key("id")])));
		assert!(!keys_end_in_id(&sort(vec![key("id"), key("y")])));
		assert!(!keys_end_in_id(&sort(vec![])));
	}

	#[test]
	fn a_loose_id_comparison_is_not_total() {
		let mut collated = key("id");
		collated.collate = true;
		let mut numeric = key("id");
		numeric.numeric = true;
		assert!(!keys_end_in_id(&sort(vec![collated])));
		assert!(!keys_end_in_id(&sort(vec![numeric])));
		let mut random = sort(vec![key("id")]);
		random.random = true;
		assert!(!keys_end_in_id(&random));
	}

	#[test]
	fn a_statement_is_total_only_when_it_sorts_by_id_and_keeps_ids_apart() {
		let by_id = sort(vec![key("y"), key("id")]);
		let by_y = sort(vec![key("y")]);
		fn scan(sorts: Vec<&SortAttrs>, repeats_ids: bool) -> ScopeScan<'_> {
			ScopeScan {
				sorts,
				repeats_ids,
			}
		}
		assert!(scope_keys_total(&scan(vec![&by_id], false)));
		assert!(scope_keys_total(&scan(vec![&by_id, &by_id], false)));
		assert!(!scope_keys_total(&scan(vec![&by_id, &by_y], false)));
		assert!(!scope_keys_total(&scan(vec![], false)));
		assert!(!scope_keys_total(&scan(vec![&by_id], true)));
	}

	#[test]
	fn a_projection_aliasing_id_is_not_total() {
		let field = |name: &str, aliased: bool| ProjectedFieldShape {
			output: FieldPath::field(name),
			aliased,
			sort_key: None,
		};
		let fields = |fields| ProjectionShape::Fields {
			fields,
			omit: Vec::new(),
		};
		assert!(renames_to_id(&fields(vec![field("id", true)])));
		assert!(!renames_to_id(&fields(vec![field("id", false), field("x", true)])));
		assert!(!renames_to_id(&ProjectionShape::All));
	}

	#[test]
	fn a_choice_ends_in_the_generic_arm() {
		let no_generic = smallvec![
			arm(Guard::SizeAtMost {
				bucket: SizeBucket::of_rows(1000)
			}),
			arm(Guard::IndexReady(index("t")))
		];
		assert!(physical_choice(no_generic, ArmPaths::Same).is_none());
		let spec = physical_choice(two_arms(), ArmPaths::Same).expect("a generic arm ends it");
		assert_eq!(spec.generic, GenericArm::Present);
		assert_eq!(spec.arms.last().map(|arm| &arm.guard), Some(&Guard::Otherwise));
	}

	#[test]
	fn a_choice_the_verifier_would_reject_is_not_built() {
		assert!(physical_choice(smallvec![arm(Guard::Otherwise)], ArmPaths::Same).is_none());
		let many: SmallVec<[ArmSpec; 4]> =
			(0..=MAX_CHOOSE_ARMS).map(|_| arm(Guard::Otherwise)).collect();
		assert!(physical_choice(many, ArmPaths::Same).is_none());
		let at_the_limit: SmallVec<[ArmSpec; 4]> = (0..MAX_CHOOSE_ARMS)
			.map(|at| {
				arm(if at + 1 == MAX_CHOOSE_ARMS {
					Guard::Otherwise
				} else {
					Guard::SizeAtMost {
						bucket: SizeBucket::of_rows(1000),
					}
				})
			})
			.collect();
		assert!(physical_choice(at_the_limit, ArmPaths::Same).is_some());
		let mut loaded = two_arms();
		loaded[0].body =
			ArmBody::UnitLoad(crate::exec::ir::iface::unit::UnitKind::DynamicTargetArm);
		assert!(physical_choice(loaded, ArmPaths::Same).is_none());
	}

	#[test]
	fn join_choice_is_absent_without_estimates() {
		let facts = facts();
		assert!(join_choice(&facts, &facts).is_none());
	}

	#[test]
	fn a_hint_guards_and_a_fact_does_not() {
		let hint = TypeKnowledge::Hint {
			kind: Kind::Int,
			source: HintSource::Annotation,
		};
		let fact = TypeKnowledge::Fact {
			kind: Kind::Int,
			source: FactSource::Literal,
		};
		assert_eq!(type_guard(&hint), Some(Guard::TypeHolds(Kind::Int)));
		assert_eq!(type_guard(&fact), None);
		assert_eq!(type_guard(&TypeKnowledge::Unknown), None);
	}
}
