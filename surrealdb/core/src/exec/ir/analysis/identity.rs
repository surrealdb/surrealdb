//! Value identity: the key under which two ops compute one value.
//!
//! Two ops are one value only when their keys are equal. An op that reads stored data carries the
//! write epoch of its read, and the effect tokens it takes, so reads a write of the statement
//! separates are two values. A clock read is constant within a statement, so equal clock reads are
//! one value.

#[cfg(test)]
use super::cx::default_cx;
use super::{effects, write_epoch};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::expr::ops::ExprTag;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::effects::Determinism;
use crate::exec::ir::iface::identity::{AttrDigest, ValueKey};
use crate::exec::ir::iface::ids::{FrameId, OpId, SnapshotId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::op_kind::OpTag;
use crate::expr::logical::EffectSet;

/// The most regions followed on the way from an op up to the top. Nesting is finite by
/// construction, so the bound is never met by a well-formed container.
const MAX_ANCESTOR_HOPS: usize = 1024;

/// The key of `op` under the default context.
///
/// Frame and snapshot are part of every key, and keys are compared only within one statement. An
/// op that `key_with` marks opaque answers `opaque: Some(op)`, which equals no key but itself. An
/// id that names no op answers an opaque key of the first tag, in the default frame and snapshot.
#[cfg(test)]
pub(crate) fn key(ir: &Ir, facts: &mut FactCache, op: OpId) -> ValueKey {
	key_with(ir, facts, &default_cx(ir), op)
}

/// The key of `op` under `cx`.
///
/// The key carries the op's tag, attribute digest, operands, frame and snapshot. Its epoch is the
/// write epoch of the op exactly when the op's transitive effects read stored data. It is opaque
/// when any of these holds:
///
/// - the attribute digest is [`AttrDigest::OPAQUE`];
/// - the transitive determinism is `NonDeterministic`;
/// - the op stands in a stored clause, which never runs where it stands;
/// - the op owns a closure body, a write stage, a transaction or a function body;
/// - the op reads a `COMPUTED` field's memo and is not deterministic.
///
/// A clock read, and a data read no write reaches, are not opaque.
pub(crate) fn key_with(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, op: OpId) -> ValueKey {
	let Some(data) = ir.op(op) else {
		return ValueKey {
			tag: OpTag::ALL[0],
			attrs: AttrDigest::OPAQUE,
			operands: Default::default(),
			tokens: Default::default(),
			frame: FrameId::new(0),
			snapshot: SnapshotId::new(0),
			epoch: None,
			opaque: Some(op),
		};
	};
	let answer = effects::summary_with(ir, facts, cx, op);
	let attrs = data.kind.attr_digest();
	let tag = data.kind.tag();
	let epoch = if answer.set.contains(EffectSet::READS_DATA) {
		Some(write_epoch::assign_with(ir, facts, cx, op))
	} else {
		None
	};
	let owns_unshared_region = data.regions.iter().any(|region| {
		ir.region(*region).is_some_and(|info| {
			matches!(info.kind, RegionKind::Transaction(_))
				|| info.kind == RegionKind::ClosureBody
				|| info.kind == RegionKind::WriteStage
				|| info.kind == RegionKind::FunctionBody
		})
	});
	let opaque = attrs == AttrDigest::OPAQUE
		|| answer.determinism == Determinism::NonDeterministic
		|| in_stored_clause(ir, op)
		|| owns_unshared_region
		|| (tag == OpTag::Expr(ExprTag::ComputedRead)
			&& answer.determinism != Determinism::Deterministic);
	ValueKey {
		tag,
		attrs,
		operands: data.operands.clone(),
		tokens: ir.token_operands(op).iter().copied().collect(),
		frame: data.frame,
		snapshot: data.snapshot,
		epoch,
		opaque: opaque.then_some(op),
	}
}

/// Whether `op` stands in a region of kind `StoredClause`, at any depth.
fn in_stored_clause(ir: &Ir, op: OpId) -> bool {
	let mut region = ir.enclosing_region(op);
	for _ in 0..MAX_ANCESTOR_HOPS {
		if let Some(info) = ir.region(region)
			&& matches!(info.kind, RegionKind::StoredClause(_))
		{
			return true;
		}
		let Some(owner) = ir.parent_op(region) else {
			return false;
		};
		region = ir.enclosing_region(owner);
	}
	false
}
