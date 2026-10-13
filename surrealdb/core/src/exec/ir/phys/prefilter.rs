//! The pre-decode prefilter of a scan.
//!
//! A scan of stored rows may judge the predicate of the filter that reads its rows on the stored
//! bytes, before a row is decoded: a record the predicate's [`PreDecodeShape`] refuses is never
//! decoded, and one the shape cannot judge is judged by the predicate itself, so the shape only
//! ever spares a record the predicate would have refused. [`run`] states the shape as the scan's
//! `prefilter` attribute, from the container the physical rewrites left.
//!
//! A scan has a prefilter exactly when all of the following hold:
//!
//! - It is a table scan of a table that is not a lightweight relation, or a key-range scan.
//! - Its rows have one reader, and that reader is a `phys.filter` that holds no predicate judged
//!   once, or a `phys.loop` of kind `Select` whose body's first op is such a filter over the loop's
//!   row. A table's enforcement, a permission or a layer between the scan and the filter puts an op
//!   there that is not a filter, so the scan has none.
//! - The filter's predicate is a conjunction, disjunction or negation of comparisons of a stored
//!   field path with a literal. A path through a dereference, an index, a method or a computed
//!   field is not stored, and a path that opens with `id` is not in the stored bytes either.
//! - The shape reads no component of a record id that the table's key structure names: such a
//!   component is derived from the id and never stored in the record body.
//!
//! The attribute is derived from the container as it stands, so a scan whose filter a rewrite
//! removed or replaced loses it. Nothing in the pass changes a result: the attribute decides how
//! much a scan decodes.

#![deny(clippy::wildcard_enum_match_arm)]

use std::sync::Arc;

use super::access::ReadVersion;
use super::conjunct::{self, def_op, strip_truthy};
use super::ops::PhysOp;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::Rewriter;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ShortCircuitOp};
use crate::exec::ir::iface::catalog::{CatalogAnswer, PhysCx, SourceKey};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::logical::ops::LoopKind;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::object_extract::PathSegment;
use crate::exec::pre_decode_filter::PreDecodeShape;
use crate::expr::{BinaryOperator, PrefixOperator};

/// States the prefilter of every scan of `ir` and answers the number of scans whose attribute
/// changed.
///
/// A table's key structure is read from the physical catalog's answers for the scan's own read
/// (the table, its `WITH` hint, the current snapshot). A table the catalog does not hold, or
/// answers as unknown, gives its scan no prefilter: the scan then decodes every row, as the
/// predicate judges it.
pub(crate) fn run(ir: &mut Ir, facts: &mut FactCache, cx: &PhysCx<'_>) -> Result<usize, IrError> {
	let mut scans = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Phys(PhysOp::TableScan(_) | PhysOp::KeyRangeScan(_)))
		) {
			scans.push(op);
		}
		Walk::Continue
	});
	let mut decided = Vec::with_capacity(scans.len());
	for scan in scans {
		let shape = prefilter_of(ir, cx, scan);
		let held = match ir.op(scan).map(|data| &data.kind) {
			Some(OpKind::Phys(PhysOp::TableScan(attrs))) => &attrs.prefilter,
			Some(OpKind::Phys(PhysOp::KeyRangeScan(attrs))) => &attrs.prefilter,
			_ => continue,
		};
		if *held != shape {
			decided.push((scan, shape));
		}
	}
	let changed = decided.len();
	let mut rw = Rewriter::new(ir, facts);
	for (scan, shape) in decided {
		rw.update_kind(scan, |kind| match kind {
			OpKind::Phys(PhysOp::TableScan(attrs)) => attrs.prefilter = shape,
			OpKind::Phys(PhysOp::KeyRangeScan(attrs)) => attrs.prefilter = shape,
			OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_) => {}
		})?;
	}
	Ok(changed)
}

/// The prefilter of the scan `scan`, or `None` where the conditions of the module do not hold.
fn prefilter_of(ir: &Ir, cx: &PhysCx<'_>, scan: OpId) -> Option<PreDecodeShape> {
	let data = ir.op(scan)?;
	let key = match &data.kind {
		OpKind::Phys(PhysOp::TableScan(attrs)) => {
			if attrs.lightweight {
				return None;
			}
			let answers: SourceKey =
				(attrs.table.clone(), attrs.read.with.clone(), ReadVersion::Current);
			match cx.physical.table_answers(&answers) {
				CatalogAnswer::Found(found) => found.key.clone(),
				CatalogAnswer::NotLoaded | CatalogAnswer::Unknown | CatalogAnswer::Absent => {
					return None;
				}
			}
		}
		OpKind::Phys(PhysOp::KeyRangeScan(attrs)) => Some(Arc::clone(&attrs.access.key)),
		OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_) => return None,
	};
	let (filter, row) = predicate_region(ir, scan)?;
	let site = conjunct::site(ir, filter)?;
	if site.once {
		return None;
	}
	let shape = shape_of(ir, row, site.root)?;
	match key.as_deref() {
		Some(key) if shape.reads_key_component(key) => None,
		Some(_) | None => Some(shape),
	}
}

/// The filter that is the first op to read the rows of the scan `scan`, with the value it reads
/// its rows as.
///
/// The rows of a scan the filter reads directly are the scan's result; those of a select loop are
/// the loop's row, the block argument its body starts from.
#[allow(clippy::wildcard_enum_match_arm)]
pub(super) fn predicate_region(ir: &Ir, scan: OpId) -> Option<(OpId, ValueId)> {
	let [rows] = ir.op(scan)?.results.as_slice() else {
		return None;
	};
	let [read] = ir.uses(*rows) else {
		return None;
	};
	if read.operand != 0 {
		return None;
	}
	let reader = ir.op(read.user)?;
	let OpKind::Phys(kind) = &reader.kind else {
		return None;
	};
	match kind {
		PhysOp::Filter => {
			let row = filter_row(ir, read.user)?;
			Some((read.user, row))
		}
		PhysOp::Loop {
			kind: LoopKind::Select,
			..
		} => {
			let [body] = <[RegionId; 1]>::try_from(reader.regions.as_slice()).ok()?;
			let element = *ir.block(*ir.region(body)?.blocks.first()?)?.args.first()?;
			let first = ops_in(ir, body).next()?;
			let filter = ir.op(first)?;
			if !matches!(filter.kind, OpKind::Phys(PhysOp::Filter))
				|| filter.operands.first() != Some(&element)
			{
				return None;
			}
			let row = filter_row(ir, first)?;
			Some((first, row))
		}
		_ => None,
	}
}

/// The block argument of the predicate region of the filter `filter`: the row it judges.
fn filter_row(ir: &Ir, filter: OpId) -> Option<ValueId> {
	let region = *ir.op(filter)?.regions.first()?;
	let block = *ir.region(region)?.blocks.first()?;
	ir.block(block)?.args.first().copied()
}

/// The shape of the predicate `value`, read from a row `row`: a conjunction, a disjunction or a
/// negation of shapes, or a comparison of a stored path with a literal.
///
/// The tree is the predicate's own: `a AND b AND c` is a conjunction of a conjunction and a leaf,
/// as the left-nested `AND` is written.
#[allow(clippy::wildcard_enum_match_arm)]
fn shape_of(ir: &Ir, row: ValueId, value: ValueId) -> Option<PreDecodeShape> {
	let data = ir.op(def_op(ir, strip_truthy(ir, value))?)?;
	let OpKind::Expr(kind) = &data.kind else {
		return None;
	};
	match (kind, data.operands.as_slice()) {
		(
			ExprOp::ShortCircuit {
				op: op @ (ShortCircuitOp::And | ShortCircuitOp::Or),
			},
			[left],
		) => {
			let right = lazy_yield(ir, data.regions.as_slice())?;
			let parts = vec![shape_of(ir, row, *left)?, shape_of(ir, row, right)?];
			Some(match op {
				ShortCircuitOp::And => PreDecodeShape::And(parts),
				_ => PreDecodeShape::Or(parts),
			})
		}
		(ExprOp::Prefix(op), [operand]) if matches!(**op, PrefixOperator::Not) => {
			Some(PreDecodeShape::Not(Box::new(shape_of(ir, row, *operand)?)))
		}
		(
			ExprOp::Binary {
				op,
			},
			[left, right],
		) => {
			if !eligible(op) {
				return None;
			}
			leaf(ir, row, *left, *right, false, op)
				.or_else(|| leaf(ir, row, *right, *left, true, op))
		}
		_ => None,
	}
}

/// The value the one `LazyOperand` region of a short-circuit operator yields.
fn lazy_yield(ir: &Ir, regions: &[RegionId]) -> Option<ValueId> {
	let [region] = regions else {
		return None;
	};
	let last = ops_in(ir, *region).last()?;
	let closing = ir.op(last)?;
	match (&closing.kind, closing.operands.as_slice()) {
		(OpKind::Expr(ExprOp::Yield), [value]) => Some(*value),
		_ => None,
	}
}

/// `field <op> literal`, or `literal <op> field` when `reversed`.
fn leaf(
	ir: &Ir,
	row: ValueId,
	field: ValueId,
	literal: ValueId,
	reversed: bool,
	op: &BinaryOperator,
) -> Option<PreDecodeShape> {
	let path = stored_path(ir, row, field)?;
	// A record's id is its key, which its stored bytes do not hold.
	if path.first()?.as_str() == "id" {
		return None;
	}
	Some(PreDecodeShape::Leaf {
		path: path.into(),
		op: op.clone(),
		literal: super::index_answer::literal(ir, literal)?,
		reversed,
	})
}

/// The path `value` reads from the row `row`, when it is a field of the row or a field of such a
/// path.
fn stored_path(ir: &Ir, row: ValueId, value: ValueId) -> Option<Vec<PathSegment>> {
	let data = ir.op(def_op(ir, value)?)?;
	let OpKind::Expr(kind) = &data.kind else {
		return None;
	};
	match (kind, data.operands.as_slice()) {
		(
			ExprOp::Field {
				name,
			},
			[base],
		) if *base == row => Some(vec![PathSegment::from(name.as_str())]),
		(
			ExprOp::Access {
				field,
			},
			[base],
		) => {
			let mut path = stored_path(ir, row, *base)?;
			path.push(PathSegment::from(field.as_str()));
			Some(path)
		}
		_ => None,
	}
}

/// Whether a comparison by `op` is one a stored value judges: the comparisons and the
/// containments, and not the ones that compare a collection with a collection.
fn eligible(op: &BinaryOperator) -> bool {
	matches!(
		op,
		BinaryOperator::Equal
			| BinaryOperator::ExactEqual
			| BinaryOperator::NotEqual
			| BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
			| BinaryOperator::Contain
			| BinaryOperator::NotContain
			| BinaryOperator::ContainAll
			| BinaryOperator::ContainAny
			| BinaryOperator::ContainNone
			| BinaryOperator::Inside
			| BinaryOperator::NotInside
			| BinaryOperator::AllInside
			| BinaryOperator::AnyInside
			| BinaryOperator::NoneInside
	)
}
