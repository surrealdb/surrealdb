//! The selectivity fact slot of a predicate.
//!
//! Selectivity is a statistics input, which the logical layer does not read. The slot exists so
//! a consumer asks one place; nothing fills it, so every answer is `None` and every consumer
//! applies its fixed rule.

use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::iface::estimate::Estimate;
use crate::exec::ir::iface::ids::OpId;

/// The estimated selectivity of the predicate `op`: always `None`.
pub(crate) fn of(_ir: &Ir, _facts: &mut FactCache, _op: OpId) -> Option<Estimate> {
	None
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::iface::region_kind::RegionKind;

	#[test]
	fn selectivity_is_absent() {
		let ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::default();
		assert_eq!(of(&ir, &mut facts, OpId::default()), None);
	}
}
