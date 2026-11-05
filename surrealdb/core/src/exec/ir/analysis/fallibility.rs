//! Transitive fallibility of an op.

#[cfg(test)]
use super::cx::default_cx;
use super::effects;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::fallibility::Fallibility;
use crate::exec::ir::iface::ids::OpId;
use crate::expr::logical::EffectSet;

/// The failure classes of `op` and of every op in its regions, under the default context.
///
/// The answer covers what the op's effects imply: a read of stored data may fail on data, an
/// opaque body on context or data, an external call on context or data. An op nothing is known
/// about answers [`Fallibility::TOP`].
#[cfg(test)]
pub(crate) fn of(ir: &Ir, facts: &mut FactCache, op: OpId) -> Fallibility {
	of_with(ir, facts, &default_cx(ir), op)
}

/// The failure classes of `op` and of every op in its regions under `cx`: the transitive
/// fallibility of [`effects::of_with`] joined with the [`floor`] of its transitive effect set.
///
/// A closure handed to a call contributes the failure classes of its body. A call's coercion
/// failure (`VALUE`) is part of the call's one local answer and is never dropped.
pub(crate) fn of_with(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, op: OpId) -> Fallibility {
	let answer = effects::summary_with(ir, facts, cx, op);
	answer.fallibility.union(floor(answer.set))
}

/// The failure classes every answer over `set` holds.
///
/// - A data read fails on data, and so does a rejection by a catalog rule.
/// - An opaque body fails on context or data.
/// - An external read or write fails on context or data.
pub(crate) fn floor(set: EffectSet) -> Fallibility {
	let mut floor = Fallibility::INFALLIBLE;
	if set.intersects(EffectSet::READS_DATA.union(EffectSet::MAY_REJECT)) {
		floor = floor.union(Fallibility::DATA);
	}
	if set.intersects(
		EffectSet::OPAQUE_BODY.union(EffectSet::READS_EXTERNAL).union(EffectSet::WRITES_EXTERNAL),
	) {
		floor = floor.union(Fallibility::CONTEXT).union(Fallibility::DATA);
	}
	floor
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn floor_follows_effects() {
		assert_eq!(floor(EffectSet::PURE), Fallibility::INFALLIBLE);
		assert_eq!(floor(EffectSet::READS_DATA), Fallibility::DATA);
		assert_eq!(floor(EffectSet::MAY_REJECT), Fallibility::DATA);
		assert_eq!(floor(EffectSet::OPAQUE_BODY), Fallibility::CONTEXT | Fallibility::DATA);
		assert_eq!(floor(EffectSet::READS_EXTERNAL), Fallibility::CONTEXT | Fallibility::DATA);
		assert_eq!(floor(EffectSet::WRITES_EXTERNAL), Fallibility::CONTEXT | Fallibility::DATA);
		assert_eq!(
			floor(EffectSet::READS_DATA | EffectSet::OPAQUE_BODY),
			Fallibility::CONTEXT | Fallibility::DATA
		);
		assert_eq!(floor(EffectSet::ENTROPY | EffectSet::CLOCK), Fallibility::INFALLIBLE);
	}
}
