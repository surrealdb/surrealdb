//! Whether an op lowers natively on a target, as the physical dialect's asserted capability.
//!
//! One capability function per target answers it for every op, so lowering asserts rather than
//! decides. This module only folds the per-op answer over an op and the ops of its regions.

use crate::exec::function::FunctionRegistry;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::op_kind::{IsaCaps, NativeLowering};
use crate::exec::vm::LeafClass;

/// How `op` lowers on `isa`, the ops of its regions included.
///
/// A unit call when any op of the subtree is one, else native with the strongest leaf class met,
/// ordered `Leaf < Kernel < ByOperand < NonLeaf`. Among equal classes the first met is kept, in
/// pre-order from `op`. A dangling id answers a unit call. The calls of the subtree resolve
/// against `registry`.
pub(crate) fn of(ir: &Ir, op: OpId, isa: IsaCaps, registry: &FunctionRegistry) -> NativeLowering {
	let Some(data) = ir.op(op) else {
		return NativeLowering::UnitCall;
	};
	let mut out = data.kind.lowers_natively(isa, registry);
	for region in data.regions.iter().copied() {
		if out == NativeLowering::UnitCall {
			break;
		}
		ir.walk(region, WalkOrder::PreOrder, &mut |inner| {
			if let Some(inner_data) = ir.op(inner) {
				out = strongest(out, inner_data.kind.lowers_natively(isa, registry));
			}
			if out == NativeLowering::UnitCall {
				Walk::Stop
			} else {
				Walk::Continue
			}
		});
	}
	out
}

/// The stronger of two lowerings: a unit call over any native one, else the greater leaf class,
/// keeping `held` on a tie.
fn strongest(held: NativeLowering, met: NativeLowering) -> NativeLowering {
	match (held, met) {
		(NativeLowering::UnitCall, _) | (_, NativeLowering::UnitCall) => NativeLowering::UnitCall,
		(
			NativeLowering::Native {
				leaf_class: held_class,
			},
			NativeLowering::Native {
				leaf_class: met_class,
			},
		) => {
			if rank(met_class) > rank(held_class) {
				met
			} else {
				held
			}
		}
	}
}

fn rank(class: LeafClass) -> u8 {
	match class {
		LeafClass::Leaf => 0,
		LeafClass::Kernel(_) => 1,
		LeafClass::ByOperand => 2,
		LeafClass::NonLeaf(_) => 3,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::iface::region_kind::RegionKind;

	fn native(class: LeafClass) -> NativeLowering {
		NativeLowering::Native {
			leaf_class: class,
		}
	}

	#[test]
	fn lowers_folds_the_strongest_class() {
		let leaf = native(LeafClass::Leaf);
		let kernel = native(LeafClass::Kernel(&[]));
		assert_eq!(strongest(leaf, kernel), kernel);
		assert_eq!(strongest(kernel, leaf), kernel);
		assert_eq!(strongest(leaf, leaf), leaf);
		assert_eq!(strongest(kernel, native(LeafClass::ByOperand)), native(LeafClass::ByOperand));
		for held in [leaf, kernel, native(LeafClass::ByOperand)] {
			assert_eq!(strongest(held, NativeLowering::UnitCall), NativeLowering::UnitCall);
			assert_eq!(strongest(NativeLowering::UnitCall, held), NativeLowering::UnitCall);
		}

		let ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		assert_eq!(
			of(
				&ir,
				OpId::default(),
				IsaCaps::default(),
				crate::exec::ir::analysis::cx::builtin_registry()
			),
			NativeLowering::UnitCall
		);
	}
}
