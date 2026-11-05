//! Whether a scan's top-k probe may reject a packed row unread.
//!
//! A table scan or key-range scan beneath a sort that publishes a threshold reads the sort's first
//! key from each record's stored bytes and refuses a record that cannot enter the kept rows
//! ([`TopKKey`]). A record stored as a document is refused that way whatever lies between the scan
//! and the sort. A packed row is read whole by every op above the scan, so a row refused unread
//! skips each of them: an op that fails on it, writes, or reads something a write could change
//! would then show the effect of the rows the probe kept and not of the rows it refused.
//!
//! [`run`] states [`TopKKey::packed_rows`] of every scan that holds a top-k key. The flag is true
//! exactly when the ops of the sort's key regions, and of the predicate of every filter the rows
//! cross between the scan and the sort, are infallible, deterministic and a function of the row
//! alone. A predicate that the scan's prefilter states whole counts as such. A flag that is false
//! only keeps the packed rows of the scan out of the probe, which the scan then decodes whole.
//! The pass recomputes the flag from the container as it stands, so it runs after the last rule
//! that changes the ops between a scan and its sort.

#![deny(clippy::wildcard_enum_match_arm)]

use super::ops::PhysOp;
use super::prefilter::predicate_region;
use super::rewrite::{beneath, keeps_scan};
use crate::exec::ir::analysis::{effects, fallibility};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::Rewriter;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::egraph::guards;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::op_kind::OpKind;

/// States the `packed_rows` flag of the top-k key of every scan of `ir` and answers the number of
/// keys whose flag changed.
///
/// A scan that holds a key and is read by no sort that publishes a threshold states `false`.
pub(crate) fn run(ir: &mut Ir, facts: &mut FactCache, cx: &EffectCx<'_>) -> Result<usize, IrError> {
	let mut scans = Vec::new();
	let mut sorts = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		match ir.op(op).map(|data| &data.kind) {
			Some(OpKind::Phys(PhysOp::TableScan(attrs))) if attrs.topk.is_some() => {
				scans.push(op);
			}
			Some(OpKind::Phys(PhysOp::KeyRangeScan(attrs))) if attrs.topk.is_some() => {
				scans.push(op);
			}
			Some(OpKind::Phys(PhysOp::Sort(attrs))) if attrs.pushdown => sorts.push(op),
			Some(_) | None => {}
		}
		Walk::Continue
	});
	let mut decided: Vec<(OpId, bool)> = scans.iter().map(|scan| (*scan, false)).collect();
	for sort in sorts {
		let Some(input) = ir.op(sort).and_then(|data| data.operands.first().copied()) else {
			continue;
		};
		let Some(chain) = beneath(ir, input, keeps_scan) else {
			continue;
		};
		let Some((_, flag)) = decided.iter_mut().find(|(scan, _)| *scan == chain.source) else {
			continue;
		};
		let regions_of = |op: OpId| -> Vec<OpId> {
			ir.op(op)
				.map(|data| data.regions.iter().flat_map(|region| ops_in(ir, *region)).collect())
				.unwrap_or_default()
		};
		// A predicate holds what it judges in its regions: the ops there are what can fail, and
		// the filter itself only drops the rows they refuse. A crossed op with no region judges
		// nothing the pass can read.
		let judged = chain.crossed.iter().all(|op| {
			if judges_whole_predicate(ir, chain.source, *op) {
				return true;
			}
			let inner = regions_of(*op);
			!inner.is_empty() && inner.into_iter().all(|inner| row_local(ir, facts, cx, inner))
		});
		*flag = judged && regions_of(sort).into_iter().all(|key| row_local(ir, facts, cx, key));
	}
	let decided: Vec<(OpId, bool)> = decided
		.into_iter()
		.filter(|(scan, flag)| match ir.op(*scan).map(|data| &data.kind) {
			Some(OpKind::Phys(PhysOp::TableScan(attrs))) => {
				attrs.topk.as_ref().is_some_and(|key| key.packed_rows != *flag)
			}
			Some(OpKind::Phys(PhysOp::KeyRangeScan(attrs))) => {
				attrs.topk.as_ref().is_some_and(|key| key.packed_rows != *flag)
			}
			Some(_) | None => false,
		})
		.collect();
	let changed = decided.len();
	let mut rw = Rewriter::new(ir, facts);
	for (scan, flag) in decided {
		rw.update_kind(scan, |kind| match kind {
			OpKind::Phys(PhysOp::TableScan(attrs)) => {
				if let Some(key) = attrs.topk.as_mut() {
					key.packed_rows = flag;
				}
			}
			OpKind::Phys(PhysOp::KeyRangeScan(attrs)) => {
				if let Some(key) = attrs.topk.as_mut() {
					key.packed_rows = flag;
				}
			}
			OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_) => {}
		})?;
	}
	Ok(changed)
}

/// Whether `filter` is the filter whose whole predicate the prefilter of `scan` states, and that
/// predicate reads nothing but fields of the row.
///
/// A prefilter is a conjunction, disjunction or negation of comparisons of stored field paths with
/// literals ([`prefilter::run`](super::prefilter)). A comparison of a field of the row is
/// infallible and reads nothing but the row, beneath every permission and layer; the effect
/// analysis answers such a read as one that may fail, since it cannot know that. A path that steps
/// into the value of a field ([`ExprOp::Access`]) may fetch the record the field links to, so a
/// predicate holding one is judged by its ops like any other.
fn judges_whole_predicate(ir: &Ir, scan: OpId, filter: OpId) -> bool {
	let stated = match ir.op(scan).map(|data| &data.kind) {
		Some(OpKind::Phys(PhysOp::TableScan(attrs))) => attrs.prefilter.is_some(),
		Some(OpKind::Phys(PhysOp::KeyRangeScan(attrs))) => attrs.prefilter.is_some(),
		Some(_) | None => false,
	};
	if !stated || predicate_region(ir, scan).is_none_or(|(held, _)| held != filter) {
		return false;
	}
	let mut steps_into_a_value = false;
	let regions = ir.op(filter).map(|data| data.regions.clone()).unwrap_or_default();
	for region in regions {
		ir.walk(region, WalkOrder::PreOrder, &mut |op| {
			if matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Expr(ExprOp::Access { .. })))
			{
				steps_into_a_value = true;
				return Walk::Stop;
			}
			Walk::Continue
		});
	}
	!steps_into_a_value
}

/// Whether skipping the evaluation of `op` for a row hides nothing: it is infallible, its two
/// evaluations are one value, and it holds no effect beyond a read of the row, under `cx`.
fn row_local(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, op: OpId) -> bool {
	let answer = effects::of_with(ir, facts, cx, op);
	guards::infallible(fallibility::of_with(ir, facts, cx, op))
		&& guards::deterministic(answer.determinism, true)
		&& guards::pure(answer.set)
}
