//! Whether the filter over an index scan rechecks what the scan's access states.
//!
//! A B-tree scan lists the records whose index entries lie in its access. The filter that is the
//! first op to read its rows judges each of those records again. When every conjunct of that filter
//! is a comparison the access holds ([`index_answer::answers`] over the columns the access states),
//! the filter rejects no record the access admits, so it is the scan's own check of each record it
//! fetches. [`run`] sets this as the scan's `rechecked` attribute, which `EXPLAIN` reads to print
//! the scan without the filter.
//!
//! The filter stays in the plan. An entry of an array column, or a value that keys like another,
//! may list a record the comparison rejects. A filter that also holds a predicate judged once is
//! not a recheck. The attribute is read from the container as it stands, so a scan whose filter a
//! rewrite changed is judged by the filter it is left with. Nothing in the pass changes a result.

#![deny(clippy::wildcard_enum_match_arm)]

use super::ops::PhysOp;
use super::{conjunct, index_answer, prefilter};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::Rewriter;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::op_kind::OpKind;

/// Sets whether the filter over each index scan of `ir` rechecks its access.
/// Returns the number of scans whose attribute changed, or the rewriter's error.
pub(crate) fn run(ir: &mut Ir, facts: &mut FactCache) -> Result<usize, IrError> {
	let mut scans = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if let Some(OpKind::Phys(PhysOp::IndexScan(attrs))) = ir.op(op).map(|data| &data.kind) {
			scans.push((op, attrs.rechecked));
		}
		Walk::Continue
	});
	let decided: Vec<(OpId, bool)> = scans
		.into_iter()
		.filter_map(|(scan, held)| {
			let rechecked = rechecked(ir, scan);
			(rechecked != held).then_some((scan, rechecked))
		})
		.collect();
	let changed = decided.len();
	let mut rw = Rewriter::new(ir, facts);
	for (scan, rechecked) in decided {
		rw.update_kind(scan, |kind| match kind {
			OpKind::Phys(PhysOp::IndexScan(attrs)) => attrs.rechecked = rechecked,
			OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_) => {}
		})?;
	}
	Ok(changed)
}

/// Whether the first op to read the rows of the index scan `scan` is a filter that judges only
/// comparisons its access states. Returns false when the scan lacks a stated access or a filter
/// site, the filter is judged once, or it has no comparisons.
fn rechecked(ir: &Ir, scan: OpId) -> bool {
	let Some(OpKind::Phys(PhysOp::IndexScan(attrs))) = ir.op(scan).map(|data| &data.kind) else {
		return false;
	};
	let Some(stated) = attrs.stated.as_deref() else {
		return false;
	};
	let Some((filter, _)) = prefilter::predicate_region(ir, scan) else {
		return false;
	};
	let Some(site) = conjunct::site(ir, filter) else {
		return false;
	};
	if site.once {
		return false;
	}
	let leaves = conjunct::leaves(ir, &site);
	!leaves.is_empty()
		&& leaves.iter().all(|leaf| index_answer::answers(ir, *leaf, &attrs.access, stated))
}
