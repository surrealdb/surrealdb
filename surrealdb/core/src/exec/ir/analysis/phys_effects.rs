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
///   `LoadUnit` and `Materialize` also the fixed table's answer. An id that names no live op, or
///   the op itself, contributes [`OpEffects::top`]: the effects of an erased op are not
///   recoverable.
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
		} else {
			of_with(ir, facts, cx, id)
		};
		out = join(&out, &named);
	}
	out
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
}
