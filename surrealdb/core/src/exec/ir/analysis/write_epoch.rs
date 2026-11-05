//! Write epochs: which reads of stored data may share a value.
//!
//! An epoch depends on the whole statement, so it is never cached per op: a write elsewhere in the
//! statement can change the answer for a read whose own operation is unchanged.

use smallvec::{SmallVec, smallvec};

#[cfg(test)]
use super::cx::default_cx;
use super::{effects, footprint};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::effects::IrResource;
use crate::exec::ir::iface::ids::{OpId, RegionId, WriteEpoch};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::expr::logical::EffectSet;

/// The most regions followed on the way from an op up to its statement. Region nesting is finite
/// in a well-formed container, so the bound is never reached by one.
const MAX_ANCESTOR_HOPS: usize = 1024;

/// The epoch of the read `op`, within its statement, under the default context.
///
/// A read whose footprint intersects any write of its statement (a triggered write included,
/// wherever it stands in program order, in any loop) answers [`WriteEpoch::Unique`] of itself,
/// which equals no other epoch. A read no write can reach shares [`WriteEpoch::Stable`].
#[cfg(test)]
pub(crate) fn assign(ir: &Ir, op: OpId) -> WriteEpoch {
	assign_with(ir, &mut FactCache::new(), &default_cx(ir), op)
}

/// The epoch of the read `op`, within its statement, under `cx`.
///
/// An op that does not read stored data answers `Stable(0)`: it has no read for a write to reach.
/// A reader whose footprint names nothing reads anything, so it meets every write. An id that
/// names no op answers `Unique(op)`.
pub(crate) fn assign_with(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	op: OpId,
) -> WriteEpoch {
	assign_scratch(ir, facts, cx, op, &mut StatementWrites::default())
}

/// The statement writes computed so far by one query, with one entry per statement.
///
/// A scratch is valid for one query over one unchanged container. Its entries are not tied to
/// the container's state, so it must be dropped before the container is rewritten.
#[derive(Default)]
pub(crate) struct StatementWrites {
	by_statement: Vec<(RegionId, SmallVec<[IrResource; 2]>)>,
}

/// [`assign_with`], reading the writes of the op's statement from `scratch` and adding them when
/// the scratch lacks them, so a query that asks about many reads of one statement folds the
/// statement once.
pub(crate) fn assign_scratch(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	op: OpId,
	scratch: &mut StatementWrites,
) -> WriteEpoch {
	if ir.op(op).is_none() {
		return WriteEpoch::Unique(op);
	}
	if !effects::summary_with(ir, facts, cx, op).set.contains(EffectSet::READS_DATA) {
		return WriteEpoch::Stable(0);
	}
	let mut reads = footprint::of_with(ir, facts, cx, op).reads;
	if reads.is_empty() {
		reads.push(IrResource::Unknown);
	}
	let writes = scratch.of(ir, facts, cx, op);
	if reads.iter().any(|read| writes.iter().any(|write| footprint::overlaps(read, write))) {
		WriteEpoch::Unique(op)
	} else {
		WriteEpoch::Stable(0)
	}
}

impl StatementWrites {
	/// The writes of the statement enclosing `op`, as [`statement_writes`] answers them.
	pub(crate) fn of(
		&mut self,
		ir: &Ir,
		facts: &mut FactCache,
		cx: &EffectCx<'_>,
		op: OpId,
	) -> &[IrResource] {
		let region = enclosing_statement(ir, op);
		let at = match self.by_statement.iter().position(|(held, _)| *held == region) {
			Some(at) => at,
			None => {
				self.by_statement.push((region, writes_of_region(ir, facts, cx, region)));
				self.by_statement.len() - 1
			}
		};
		&self.by_statement[at].1
	}
}

/// Every write of the statement enclosing `op`, triggered writes included.
///
/// The statement is the outermost enclosing region of kind `Statement`, or the container's top
/// region for an op outside every statement. The answer is the writes of every op of that region,
/// stored clauses excluded and the bodies of closures counted where the closure is invoked. A list
/// holding `IrResource::Unknown` is that resource alone.
pub(crate) fn statement_writes(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	op: OpId,
) -> SmallVec<[IrResource; 2]> {
	writes_of_region(ir, facts, cx, enclosing_statement(ir, op))
}

fn writes_of_region(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	region: RegionId,
) -> SmallVec<[IrResource; 2]> {
	let mut out: SmallVec<[IrResource; 2]> = SmallVec::new();
	for child in ops_in(ir, region) {
		for write in effects::of_with(ir, facts, cx, child).footprint.writes {
			if write == IrResource::Unknown {
				return smallvec![IrResource::Unknown];
			}
			if !out.contains(&write) {
				out.push(write);
			}
		}
	}
	out
}

/// The outermost region of kind `Statement` that holds `op`, else the top region.
fn enclosing_statement(ir: &Ir, op: OpId) -> RegionId {
	let mut region = ir.enclosing_region(op);
	let mut found = None;
	for _ in 0..MAX_ANCESTOR_HOPS {
		if let Some(info) = ir.region(region)
			&& matches!(info.kind, RegionKind::Statement(_))
		{
			found = Some(region);
		}
		let Some(owner) = ir.parent_op(region) else {
			break;
		};
		region = ir.enclosing_region(owner);
	}
	found.unwrap_or_else(|| ir.top())
}
