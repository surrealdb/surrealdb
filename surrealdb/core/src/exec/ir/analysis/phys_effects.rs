//! The effects of a physical op, derived from the logical ops it stands for.
//!
//! A physical op's effects, footprint, rollback behaviour and fallibility are the fold of the
//! transitive answers of the logical ops its provenance names. [`of`] is the fixed table by
//! variant: the containers and control ops answer the bottom of the lattice, since their regions
//! answer for themselves; a prefetch, a unit load and a materialisation answer for what they do
//! themselves; every other variant answers the top, since its answer is reachable only through
//! its provenance. The table reads no catalog and takes only the effect context.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::smallvec;

use super::effects::{bottom, join, of_with};
use crate::exec::ir::core::Provenance;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::effects::{Determinism, IrFootprint, IrResource, OpEffects, Rollback};
use crate::exec::ir::iface::fallibility::Fallibility;
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::logical::ops::LoopKind;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::{PhysOp, PhysTag};
use crate::expr::logical::EffectSet;

/// The effects of the physical op `op`, by variant alone.
///
/// The match names every variant, so a variant added to the dialect does not compile until its
/// effects and fallibility are declared here.
///
/// - The containers and control ops answer [`bottom`]: their regions answer for themselves. A loop
///   of kind `GraphStep` reads the records its lookup reaches, so it answers [`OpEffects::top`].
/// - `Prefetch` reads stored data it cannot name, raises nothing and leaves nothing to undo.
/// - `LoadUnit` reads the catalog and raises nothing.
/// - `Materialize` holds rows it was handed and has no effect of its own.
/// - Every other variant answers [`OpEffects::top`]: its answer is the fold of its provenance,
///   which [`of_op`] reads from the container.
pub(crate) fn of(op: &PhysOp, _cx: &EffectCx<'_>) -> OpEffects {
	match op {
		PhysOp::Loop {
			kind: LoopKind::GraphStep,
			..
		}
		| PhysOp::Statement {
			served: Some(_),
			..
		} => OpEffects::top(),
		PhysOp::Pipeline(..)
		| PhysOp::Block
		| PhysOp::If {
			..
		}
		| PhysOp::Loop {
			..
		}
		| PhysOp::Choose(..)
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
			served: None,
			..
		}
		| PhysOp::Yield
		| PhysOp::Return
		| PhysOp::Break
		| PhysOp::Next
		| PhysOp::Materialize {
			..
		} => bottom(),
		PhysOp::Prefetch(..) => OpEffects {
			set: EffectSet::READS_DATA,
			fallibility: Fallibility::INFALLIBLE,
			context_deps: smallvec![],
			footprint: IrFootprint {
				reads: smallvec![IrResource::Unknown],
				writes: smallvec![],
			},
			rollback: Rollback::NoEffects,
			determinism: Determinism::WithinStatementUnlessWritten,
		},
		PhysOp::LoadUnit(..) => OpEffects {
			set: EffectSet::READS_CATALOG,
			..bottom()
		},
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
		| PhysOp::Let(..)
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
		| PhysOp::Sleep => OpEffects::top(),
	}
}

/// The local effects of the op `op` of `ir`: the fold of the transitive answers of the logical
/// ops its provenance names, joined with the fixed table's answer for the variants that do
/// something themselves.
///
/// - `Provenance::Bound`, and a `From` set that names nothing, answer [`of`].
/// - `Provenance::From(ids)` answers the [`join`] over [`of_with`] of every id, and for `Prefetch`,
///   `LoadUnit` and `Materialize` also the fixed table's answer. An id that names a live op
///   contributes that op's transitive answer. An id of an op the lowering replaced contributes the
///   answer the analysis recorded for it under `cx` itself ([`FactCache::last_under`]) when that
///   answer leaves nothing behind once the statement is done, and [`OpEffects::top`] otherwise: an
///   answer recorded under another context never answers, and neither does the answer of a write,
///   whose footprint widens with the stored code its stage ops do or do not show, which the record
///   does not carry. The op itself contributes [`OpEffects::top`]. The recorded answer is the
///   replaced op's region-local one, which leaves out the bodies of the closures its ops invoke; it
///   still states unknown writes for them, since every op that invokes a closure or runs stored
///   code the plan does not show states them as its own effect.
/// - An op that is not physical answers its own kind's local effects, and a dangling `op` answers
///   [`OpEffects::top`].
pub(crate) fn of_op(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, op: OpId) -> OpEffects {
	let Some(data) = ir.op(op) else {
		return OpEffects::top();
	};
	let OpKind::Phys(kind) = &data.kind else {
		return data.kind.local_effects(cx);
	};
	let fixed = of(kind, cx);
	if is_container_or_control(kind) {
		return fixed;
	}
	let ids = match &data.provenance {
		Provenance::From(ids) if !ids.is_empty() => ids,
		Provenance::Bound | Provenance::From(_) => return fixed,
	};
	let mut out = if answers_for_itself(kind) {
		fixed
	} else {
		bottom()
	};
	for id in ids.iter().copied() {
		let named = if id == op {
			OpEffects::top()
		} else if ir.op(id).is_none() {
			let key = cx.memo_key();
			facts
				.last_under(id, &key)
				.filter(|kept| !kept.effects.set.mutates_state())
				.map_or_else(OpEffects::top, |kept| kept.effects.clone())
		} else {
			of_with(ir, facts, cx, id)
		};
		out = join(&out, &named);
	}
	out
}

/// Whether `kind` is a region container or control terminator whose own local effect is [`bottom`]:
/// the regions it owns are walked directly by [`super::effects`] and [`super::footprint`], so a
/// provenance that names the logical container it replaced does not contribute to its local effect.
fn is_container_or_control(kind: &PhysOp) -> bool {
	match kind {
		PhysOp::Pipeline(..)
		| PhysOp::Block
		| PhysOp::If {
			..
		}
		| PhysOp::Choose(..)
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
			served: None,
			..
		}
		| PhysOp::Yield
		| PhysOp::Return
		| PhysOp::Break
		| PhysOp::Next => true,
		PhysOp::Loop {
			kind: LoopKind::GraphStep,
			..
		} => false,
		PhysOp::Loop {
			..
		} => true,
		PhysOp::Statement {
			served: Some(_),
			..
		}
		| PhysOp::Materialize {
			..
		}
		| PhysOp::Prefetch(..)
		| PhysOp::LoadUnit(..)
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
		| PhysOp::Let(..)
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
		| PhysOp::Sleep => false,
	}
}

/// Whether the variant's own effects are not among those of the logical ops it stands for.
fn answers_for_itself(kind: &PhysOp) -> bool {
	let tag = kind.tag();
	tag == PhysTag::Prefetch || tag == PhysTag::LoadUnit || tag == PhysTag::Materialize
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::ir::iface::region_kind::{RegionKind, StatementCommit};
	use crate::exec::ir::iface::unit::{LoadUnitSpec, UnitKind, UnitStaticKey};
	use crate::exec::ir::logical::ops::LoopKind;
	use crate::exec::ir::phys::ops::{Batch, MaterializeImage, PrefetchKeys};

	fn with_cx<R>(f: impl FnOnce(&EffectCx<'_>) -> R) -> R {
		let frames = FrameTable::default();
		let cx = EffectCx {
			registry: super::super::cx::builtin_registry(),
			catalog: &super::super::cx::NoDefinitions,
			frames: &frames,
		};
		f(&cx)
	}

	#[test]
	fn phys_table_is_exhaustive() {
		with_cx(|cx| {
			let containers = [
				PhysOp::Block,
				PhysOp::If {
					value: false,
				},
				PhysOp::Loop {
					kind: LoopKind::Select,
					first: false,
					batch: Batch {
						max_iterations: 1,
						max_bytes: 1,
					},
					unroll: 1,
					value: false,
					step: None,
				},
				PhysOp::Request {
					fragment: None,
					nested: false,
				},
				PhysOp::Stmt {
					commit: StatementCommit::Auto,
					control: None,
				},
				PhysOp::Yield,
				PhysOp::Return,
				PhysOp::Break,
				PhysOp::Next,
				PhysOp::Materialize {
					image: MaterializeImage::KeyAndValue,
				},
			];
			for op in &containers {
				assert_eq!(of(op, cx), bottom(), "{:?} answers the bottom", op.tag());
			}

			let prefetch = of(
				&PhysOp::Prefetch(Box::new(PrefetchKeys {
					points: Default::default(),
					ranges: Default::default(),
				})),
				cx,
			);
			assert!(prefetch.fallibility.is_infallible());
			assert_eq!(prefetch.set, EffectSet::READS_DATA);
			assert_eq!(prefetch.rollback, Rollback::NoEffects);
			assert_eq!(prefetch.determinism, Determinism::WithinStatementUnlessWritten);
			assert_eq!(prefetch.footprint.reads.as_slice(), &[IrResource::Unknown]);
			assert!(prefetch.footprint.writes.is_empty());

			let load = of(
				&PhysOp::LoadUnit(Box::new(LoadUnitSpec {
					kind: UnitKind::Function,
					static_key: UnitStaticKey::Shape(0),
				})),
				cx,
			);
			assert!(load.fallibility.is_infallible());
			assert_eq!(load.set, EffectSet::READS_CATALOG);

			assert_eq!(of(&PhysOp::Filter, cx), OpEffects::top());
			assert_eq!(of(&PhysOp::Sleep, cx), OpEffects::top());
		});
	}

	/// A physical op that stands for logical ops the lowering replaced answers what the analysis
	/// stated for them, and the top where it stated nothing.
	#[test]
	fn a_physical_op_answers_the_effects_stated_for_the_logical_op_it_replaced() {
		use crate::exec::ir::core::location::{LocOrigin, Location};
		use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
		use crate::exec::ir::expr::ops::ExprOp;
		use crate::exec::ir::iface::conversion::EliminationReason;
		use crate::exec::ir::iface::ids::{FrameId, SnapshotId};
		use crate::exec::ir::iface::types::TypeKnowledge;
		use crate::exec::ir::logical::ops::LogOp;

		fn spec(kind: OpKind) -> OpSpec {
			OpSpec {
				kind,
				operands: Default::default(),
				results: std::iter::empty::<TypeKnowledge>().collect(),
				frame: FrameId::new(0),
				snapshot: SnapshotId::new(0),
				loc: Location {
					stmt: 0,
					span: None,
					origin: LocOrigin::Synthesized,
				},
			}
		}

		let mut ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let block = ir.region(ir.top()).expect("the top region exists").blocks[0];
		let mut facts = FactCache::default();
		let (stated, unstated, opaque, writer) = {
			let mut rw = Rewriter::new(&mut ir, &mut facts)
				.with_registry(super::super::cx::builtin_registry());
			let fetch = || spec(OpKind::Logical(LogOp::Fetch));
			let stated = rw.create(InsertPoint::End(block), fetch()).expect("the op is created");
			let unstated = rw.create(InsertPoint::End(block), fetch()).expect("the op is created");
			let opaque = rw
				.create(InsertPoint::End(block), spec(OpKind::Expr(ExprOp::Invoke)))
				.expect("the op is created");
			let writer = rw
				.create(InsertPoint::End(block), spec(OpKind::Logical(LogOp::Kill)))
				.expect("the op is created");
			let recorded = rw.facts(stated).effects;
			assert_eq!(recorded.set, EffectSet::READS_DATA);
			rw.facts(opaque);
			assert!(rw.facts(writer).effects.set.mutates_state());
			for erased in [stated, unstated, opaque, writer] {
				rw.erase(erased, EliminationReason::DeadPure).expect("the op is erased");
			}
			(stated, unstated, opaque, writer)
		};
		let (replacing_stated, replacing_unstated, replacing_opaque, replacing_writer) = {
			let mut rw = Rewriter::new(&mut ir, &mut facts);
			let mut replacing = |of: OpId| {
				rw.derived(&[of], |rw| {
					rw.create(InsertPoint::End(block), spec(OpKind::Phys(PhysOp::Filter)))
						.expect("the op is created")
				})
			};
			(replacing(stated), replacing(unstated), replacing(opaque), replacing(writer))
		};
		let cx = super::super::cx::default_cx(&ir);
		let effects = of_op(&ir, &mut facts, &cx, replacing_stated);
		assert_eq!(effects.set, EffectSet::READS_DATA);
		assert!(effects.footprint.writes.is_empty(), "a read writes nothing");
		assert_eq!(of_op(&ir, &mut facts, &cx, replacing_unstated), OpEffects::top());
		// An op that runs a body the plan does not show states unknown writes, so the op that
		// replaces it does too.
		let effects = of_op(&ir, &mut facts, &cx, replacing_opaque);
		assert!(effects.set.contains(EffectSet::OPAQUE_BODY));
		assert_eq!(effects.footprint.writes.as_slice(), &[IrResource::Unknown]);
		// A write's recorded answer does not carry the stored code its stage ops show, so the op
		// that replaces a write answers the top.
		assert_eq!(of_op(&ir, &mut facts, &cx, replacing_writer), OpEffects::top());
		// Under a context the analysis never ran under, a replaced op's answer is not known.
		let other = FrameTable::default();
		let elsewhere = EffectCx {
			frames: &other,
			..cx
		};
		assert_eq!(of_op(&ir, &mut facts, &elsewhere, replacing_stated), OpEffects::top());
	}

	#[test]
	fn a_dangling_op_answers_the_top() {
		let ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::default();
		with_cx(|cx| {
			assert_eq!(of_op(&ir, &mut facts, cx, OpId::default()), OpEffects::top());
		});
	}

	#[test]
	fn container_and_control_ops_answer_bottom_locally_and_served_statement_answers_top() {
		use crate::exec::ir::core::location::{LocOrigin, Location};
		use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
		use crate::exec::ir::iface::conversion::EliminationReason;
		use crate::exec::ir::iface::ids::{FrameId, SnapshotId};
		use crate::exec::ir::iface::types::TypeKnowledge;
		use crate::exec::ir::logical::ops::LogOp;
		use crate::exec::planner::CatalogWithin;

		fn spec(kind: OpKind) -> OpSpec {
			OpSpec {
				kind,
				operands: Default::default(),
				results: std::iter::empty::<TypeKnowledge>().collect(),
				frame: FrameId::new(0),
				snapshot: SnapshotId::new(0),
				loc: Location {
					stmt: 0,
					span: None,
					origin: LocOrigin::Synthesized,
				},
			}
		}

		with_cx(|cx| {
			assert_eq!(
				of(
					&PhysOp::Statement {
						changes_catalog: false,
						served: None,
						within: CatalogWithin::default(),
						raises_when_reached: false,
					},
					cx,
				),
				bottom(),
			);
			assert_eq!(
				of(
					&PhysOp::Statement {
						changes_catalog: false,
						served: Some(0),
						within: CatalogWithin::default(),
						raises_when_reached: false,
					},
					cx,
				),
				OpEffects::top(),
			);
		});

		let mut ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let block = ir.region(ir.top()).expect("the top region exists").blocks[0];
		let mut facts = FactCache::default();
		let (logical_stmt, phys_stmt, phys_served) = {
			let mut rw = Rewriter::new(&mut ir, &mut facts);
			let logical_stmt = rw
				.create(
					InsertPoint::End(block),
					spec(OpKind::Logical(LogOp::Stmt {
						commit: StatementCommit::Auto,
						control: None,
					})),
				)
				.expect("the op is created");
			rw.erase(logical_stmt, EliminationReason::DeadPure).expect("the op is erased");
			let phys_stmt = rw.derived(&[logical_stmt], |rw| {
				rw.create(
					InsertPoint::End(block),
					spec(OpKind::Phys(PhysOp::Stmt {
						commit: StatementCommit::Auto,
						control: None,
					})),
				)
				.expect("the op is created")
			});
			let phys_served = rw.derived(&[logical_stmt], |rw| {
				rw.create(
					InsertPoint::End(block),
					spec(OpKind::Phys(PhysOp::Statement {
						changes_catalog: false,
						served: Some(0),
						within: CatalogWithin::default(),
						raises_when_reached: false,
					})),
				)
				.expect("the op is created")
			});
			(logical_stmt, phys_stmt, phys_served)
		};
		assert!(ir.op(logical_stmt).is_none());
		let cx = super::super::cx::default_cx(&ir);
		assert_eq!(of_op(&ir, &mut facts, &cx, phys_stmt), bottom());
		assert_eq!(of_op(&ir, &mut facts, &cx, phys_served), OpEffects::top());
	}
}
