//! Whether an index-backed KNN search builds its rows from the record ids it returns.
//!
//! A `phys.knn_scan` whose rows need nothing of their records but the id need not read them: the
//! search already holds each id. [`run`] states this as the scan's `ids_only` attribute, from the
//! container the physical rewrites left. A scan is ids-only exactly when all of the following
//! hold:
//!
//! - It reads the current version, and its search judges no candidate on its record (it has no
//!   filter).
//! - Its rows have one reader, a `phys.loop` of kind `Select` over the scan's result, whose body is
//!   a projection of field reads and literals and nothing else: a call or a method may read the row
//!   without naming a field (`type::field`), and the table's permission and its layers read the
//!   record.
//! - The loop's body reads no field of a row but `id`
//!   ([`liveness::pipeline_demand`](crate::exec::ir::analysis::liveness::pipeline_demand)).
//! - Every definition of the table's `id` field reads as stored: it is not `COMPUTED` and its
//!   `SELECT` permission is `FULL`.
//!
//! Nothing in the pass changes a result: the attribute decides whether the records are read.

#![deny(clippy::wildcard_enum_match_arm)]

use super::access::ReadVersion;
use super::ops::PhysOp;
use crate::exec::ir::analysis::{liveness, multiplicity};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::Rewriter;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::catalog::{CatalogAnswer, PhysCx, SourceKey};
use crate::exec::ir::iface::effects::FieldSet;
use crate::exec::ir::iface::frame::Snapshot;
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::logical::ops::LoopKind;
use crate::exec::ir::op_kind::OpKind;

/// States whether every KNN scan of `ir` builds its rows from ids, and answers the number of scans
/// whose attribute changed.
pub(crate) fn run(ir: &mut Ir, facts: &mut FactCache, cx: &PhysCx<'_>) -> Result<usize, IrError> {
	let mut scans = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if let Some(OpKind::Phys(PhysOp::KnnScan(attrs))) = ir.op(op).map(|data| &data.kind) {
			scans.push((op, attrs.ids_only));
		}
		Walk::Continue
	});
	let decided: Vec<(OpId, bool)> = scans
		.into_iter()
		.filter_map(|(scan, held)| {
			let ids_only = ids_only(ir, cx, scan);
			(ids_only != held).then_some((scan, ids_only))
		})
		.collect();
	let changed = decided.len();
	let mut rw = Rewriter::new(ir, facts);
	for (scan, ids_only) in decided {
		rw.update_kind(scan, |kind| {
			if let OpKind::Phys(PhysOp::KnnScan(attrs)) = kind {
				attrs.ids_only = ids_only;
			}
		})?;
	}
	Ok(changed)
}

/// Whether the KNN scan `scan` meets the conditions of the module.
fn ids_only(ir: &Ir, cx: &PhysCx<'_>, scan: OpId) -> bool {
	let Some(data) = ir.op(scan) else {
		return false;
	};
	let OpKind::Phys(PhysOp::KnnScan(attrs)) = &data.kind else {
		return false;
	};
	if attrs.filter.is_some() || !matches!(ir.snapshot(data.snapshot), Some(Snapshot::Current)) {
		return false;
	}
	let [rows] = data.results.as_slice() else {
		return false;
	};
	let [read] = ir.uses(*rows) else {
		return false;
	};
	let reader = read.user;
	let is_select = matches!(
		ir.op(reader).map(|data| &data.kind),
		Some(OpKind::Phys(PhysOp::Loop {
			kind: LoopKind::Select,
			..
		}))
	);
	if read.operand != 0 || !is_select {
		return false;
	}
	let (Some(body), Some(element)) =
		(multiplicity::loop_body(ir, reader), multiplicity::loop_element(ir, reader))
	else {
		return false;
	};
	// Only a projection of field reads off the row is followed: a call, a method or any other op
	// may read the row without naming a field (`type::field`), and a permission filter or a layer
	// reads the record.
	let mut opaque = false;
	ir.walk(body, WalkOrder::PreOrder, &mut |op| {
		let plain = matches!(
			ir.op(op).map(|data| &data.kind),
			Some(
				OpKind::Phys(PhysOp::Project(_) | PhysOp::Yield)
					| OpKind::Expr(
						ExprOp::Field { .. }
							| ExprOp::Access { .. }
							| ExprOp::Doc | ExprOp::Literal(_)
							| ExprOp::Yield
					)
			)
		);
		if !plain {
			opaque = true;
			return Walk::Stop;
		}
		Walk::Continue
	});
	if opaque {
		return false;
	}
	// The body's ops before the `phys.yield` that ends it, which must yield what the last of them
	// delivers: the demand then follows every row the body hands on.
	let mut steps: Vec<OpId> = ops_in(ir, body).collect();
	let Some(terminator) = steps.pop() else {
		return false;
	};
	let Some(end) = ir.op(terminator) else {
		return false;
	};
	let last = steps.last().and_then(|op| ir.op(*op)).and_then(|data| data.results.first());
	if !matches!(end.kind, OpKind::Phys(PhysOp::Yield)) || end.operands.first() != last {
		return false;
	}
	let demand = liveness::pipeline_demand(ir, element, &[(element, &steps)]);
	let reads_only_id = if let FieldSet::Some(names) = &demand.stored {
		names.iter().all(|name| name.as_str() == "id")
	} else {
		false
	};
	reads_only_id && demand.computed.is_empty() && id_reads_as_stored(cx, attrs)
}

/// Whether every definition of the `id` field of the scanned table reads as stored, as the
/// physical catalog answers for the scan's read.
fn id_reads_as_stored(cx: &PhysCx<'_>, attrs: &super::ops::KnnScanAttrs) -> bool {
	use crate::expr::part::Part;
	let key: SourceKey = (attrs.table.clone(), attrs.read.with.clone(), ReadVersion::Current);
	let CatalogAnswer::Found(answers) = cx.physical.table_answers(&key) else {
		return false;
	};
	answers
		.fields
		.iter()
		.filter(|field| {
			if let [Part::Field(root), rest @ ..] = field.name.0.as_slice()
				&& rest.iter().all(|part| matches!(part, Part::Field(_) | Part::All))
			{
				return root.as_str() == "id";
			}
			true
		})
		.all(|field| {
			field.computed.is_none()
				&& matches!(field.select_permission, crate::catalog::Permission::Full)
		})
}
