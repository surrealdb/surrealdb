//! The authority frames of a container: the check that every op runs under the frame its
//! position implies.
//!
//! A frame region reads no caller value that was not passed to it as an operand, and an op's
//! frame is identity-bearing everywhere it is compared.
//!
//! The same module answers the three questions a permission filter and a projection layer pose
//! to the rewrites that move work across them: whether a statement's writes can change what the
//! permission predicate reads ([`merge_ok`]), which view of the row a conjunct needs
//! ([`row_view`]), and whether the layer's per-row work can be skipped for a row without anyone
//! noticing ([`layer_work_unobservable`]). Field guards are catalog facts the analysis cannot
//! read, so a field read through the layer is a masked read and the layer's work is never
//! unobservable.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;
use surrealdb_strand::TableName;

#[cfg(test)]
use super::cx::default_cx;
use super::liveness::{demand_over, field_read_name};
use super::{effects, fallibility, footprint, multiplicity, write_epoch};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::effects::{FieldSet, IrResource};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId, WriteEpoch};
use crate::exec::ir::iface::region_kind::RegionProperties;
use crate::exec::ir::logical::ops::{LogOp, LogTag};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::expr::logical::EffectSet;

/// The most regions followed on the way up from a value's definition. Nesting is finite by
/// construction, so a well-formed container never meets the bound.
const MAX_HOPS: usize = 1024;

/// The effects a layer's per-row work may have and still go unnoticed when it is skipped.
const UNOBSERVABLE: EffectSet =
	EffectSet::READS_ROW.union(EffectSet::READS_DATA).union(EffectSet::READS_CATALOG);

/// An op that reads a value no isolated region may read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FrameViolation {
	/// The op that takes the operand.
	pub(crate) op: OpId,
	/// The value defined outside the isolated region and not passed to it.
	pub(crate) value: ValueId,
}

/// Every isolated region reads only its own block arguments, values defined inside it, and its
/// owner's operands.
///
/// The check walks every region of the container whose kind is isolated from above, and every op
/// beneath it. The answer is the first op found that reads any other value, which includes a
/// value that names nothing. A container with no such op answers `Ok`.
pub(crate) fn check(ir: &Ir) -> Result<(), FrameViolation> {
	let mut found = None;
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		let Some(owner) = ir.op(op) else {
			return Walk::Continue;
		};
		for region in owner.regions.iter().copied() {
			let isolated = ir.region(region).is_some_and(|info| info.kind.isolated_from_above());
			if isolated && let Some(violation) = check_region(ir, op, region) {
				found = Some(violation);
				return Walk::Stop;
			}
		}
		Walk::Continue
	});
	match found {
		Some(violation) => Err(violation),
		None => Ok(()),
	}
}

/// The first op beneath `region`, an isolated region of `owner`, that reads a value defined
/// outside `region` and not among the operands of `owner`.
fn check_region(ir: &Ir, owner: OpId, region: RegionId) -> Option<FrameViolation> {
	let passed: SmallVec<[ValueId; 4]> =
		ir.op(owner).map(|data| data.operands.iter().copied().collect()).unwrap_or_default();
	let mut found = None;
	ir.walk(region, WalkOrder::PreOrder, &mut |op| {
		let Some(data) = ir.op(op) else {
			return Walk::Continue;
		};
		for value in data.operands.iter().copied() {
			if passed.contains(&value) || defined_within(ir, value, region) {
				continue;
			}
			found = Some(FrameViolation {
				op,
				value,
			});
			return Walk::Stop;
		}
		Walk::Continue
	});
	found
}

/// Whether `value` is defined in `region` or in a region beneath it.
fn defined_within(ir: &Ir, value: ValueId, region: RegionId) -> bool {
	let Some(data) = ir.value(value) else {
		return false;
	};
	let mut current = match data.def {
		ValueDef::OpResult {
			op,
			..
		} => {
			if ir.op(op).is_none() {
				return false;
			}
			ir.enclosing_region(op)
		}
		ValueDef::BlockArg {
			block,
			..
		} => match ir.block(block) {
			Some(block) => block.parent,
			None => return false,
		},
	};
	for _ in 0..MAX_HOPS {
		if current == region {
			return true;
		}
		let Some(owner) = ir.parent_op(current) else {
			return false;
		};
		current = ir.enclosing_region(owner);
	}
	false
}

/// The conditions under which a permission predicate may be answered ahead of the rows it filters.
/// `condition1` is the write-independence condition: no write of the statement changes what the
/// predicate reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MergeOk {
	/// No write of the statement changes what the predicate reads.
	pub(crate) condition1: bool,
}

/// Whether no write of the statement reaches what the permission predicate of `permission_filter`
/// reads, under the default context.
#[cfg(test)]
pub(crate) fn merge_ok(ir: &Ir, facts: &mut FactCache, permission_filter: OpId) -> MergeOk {
	merge_ok_with(ir, facts, &default_cx(ir), permission_filter)
}

/// Whether no write of the statement reaches what the permission predicate of `permission_filter`
/// reads, under `cx`.
///
/// `condition1` holds when every read of stored data in the predicate's regions has a stable
/// epoch, and no write of the statement names the permission's table with fields that meet the
/// fields the predicate reads off the row. The table is the table the enclosing loop scans; when
/// it is unknown every table counts. An op that is not a permission filter, a write whose
/// footprint is unknown and a predicate that reads the whole row answer `false`.
pub(crate) fn merge_ok_with(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	permission_filter: OpId,
) -> MergeOk {
	MergeOk {
		condition1: condition1(ir, facts, cx, permission_filter),
	}
}

fn condition1(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, filter: OpId) -> bool {
	let Some(data) = ir.op(filter) else {
		return false;
	};
	if data.kind.tag() != OpTag::Logical(LogTag::PermissionFilter) {
		return false;
	}
	let regions: SmallVec<[RegionId; 2]> = data.regions.iter().copied().collect();
	let mut scratch = write_epoch::StatementWrites::default();
	for region in regions.iter().copied() {
		for child in ops_in(ir, region) {
			if let WriteEpoch::Unique(_) =
				write_epoch::assign_scratch(ir, facts, cx, child, &mut scratch)
			{
				return false;
			}
		}
	}
	let writes = scratch.of(ir, facts, cx, filter);
	if writes.contains(&IrResource::Unknown) {
		return false;
	}
	let table = permission_table(ir, filter);
	let mut seeds: SmallVec<[ValueId; 2]> = SmallVec::new();
	for region in regions.iter().copied() {
		if let Some(info) = ir.region(region) {
			for block in info.blocks.iter().copied() {
				if let Some(block) = ir.block(block) {
					seeds.extend(block.args.iter().copied());
				}
			}
		}
	}
	let read = demand_over(ir, &seeds, &regions).stored;
	!writes.iter().any(|write| match write {
		IrResource::Data {
			table: written,
			fields,
			..
		} => tables_meet(table.as_ref(), written.as_ref()) && fields_meet(fields, &read),
		IrResource::Catalog {
			..
		}
		| IrResource::Snapshot(_)
		| IrResource::Cascade {
			..
		}
		| IrResource::Reject {
			..
		}
		| IrResource::Queue {
			..
		}
		| IrResource::Session {
			..
		}
		| IrResource::Unknown => false,
	})
}

/// The table whose rows the loop holding `filter` scans, if the loop's source names one.
fn permission_table(ir: &Ir, filter: OpId) -> Option<TableName> {
	let scan = footprint::enclosing_loop(ir, filter)?;
	let source = *ir.op(scan)?.operands.first()?;
	multiplicity::source_table(ir, source, 0)
}

/// Whether two table names may denote one table: an unnamed side matches any.
fn tables_meet(a: Option<&TableName>, b: Option<&TableName>) -> bool {
	match (a, b) {
		(Some(a), Some(b)) => a == b,
		_ => true,
	}
}

/// Whether two field sets share a field; an empty set shares none.
fn fields_meet(a: &FieldSet, b: &FieldSet) -> bool {
	match (a, b) {
		(FieldSet::All, FieldSet::All) => true,
		(FieldSet::All, FieldSet::Some(fields)) | (FieldSet::Some(fields), FieldSet::All) => {
			!fields.is_empty()
		}
		(FieldSet::Some(a), FieldSet::Some(b)) => a.iter().any(|field| b.contains(field)),
	}
}

/// The view of a row a conjunct reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RowView {
	/// The row as the layer passes it on: guarded fields hidden, `COMPUTED` fields derived.
	Masked,
	/// The stored row. A conjunct that reads only this may run below the layer.
	Real,
}

/// The view of the row the conjunct `conjunct` needs, for the projection layer `layer`.
///
/// `Real` when every row read of the conjunct is an `expr.field` over the row the layer takes in,
/// and the conjunct reads no whole row; a conjunct that reads no row is `Real`. Any read of the
/// row the layer delivers is `Masked`, since the guards of its fields cannot be read, and so is
/// any whole-row use, a `COMPUTED` read and a closure or call that takes the row. An op that is
/// not a layer answers `Masked`.
///
/// The conjunct is the op that yields the conjunct's value together with the ops that compute it:
/// its operands defined in its own region, and everything in the regions those ops own.
pub(crate) fn row_view(ir: &Ir, _facts: &mut FactCache, conjunct: OpId, layer: OpId) -> RowView {
	let Some(layer_data) = ir.op(layer) else {
		return RowView::Masked;
	};
	let is_layer = if let OpKind::Logical(LogOp::Project(attrs)) = &layer_data.kind {
		attrs.layer
	} else {
		false
	};
	let Some(real) = layer_data.operands.first().copied() else {
		return RowView::Masked;
	};
	if !is_layer || ir.op(conjunct).is_none() {
		return RowView::Masked;
	}
	let masked: SmallVec<[ValueId; 2]> = layer_data.results.iter().copied().collect();
	let home = ir.enclosing_region(conjunct);
	let mut seen: FxHashSet<OpId> = FxHashSet::default();
	let mut work = vec![conjunct];
	while let Some(op) = work.pop() {
		if !seen.insert(op) {
			continue;
		}
		let Some(data) = ir.op(op) else {
			return RowView::Masked;
		};
		let mut inner: Vec<OpId> = vec![op];
		for region in data.regions.iter().copied() {
			ir.walk(region, WalkOrder::PreOrder, &mut |nested| {
				inner.push(nested);
				Walk::Continue
			});
		}
		for member in inner {
			let Some(member_data) = ir.op(member) else {
				return RowView::Masked;
			};
			for (position, operand) in member_data.operands.iter().copied().enumerate() {
				if masked.contains(&operand) {
					return RowView::Masked;
				}
				if operand == real
					&& (position != 0 || field_read_name(&member_data.kind).is_none())
				{
					return RowView::Masked;
				}
				if let Some(ValueDef::OpResult {
					op: defining,
					..
				}) = ir.value(operand).map(|value| value.def)
					&& ir.op(defining).is_some()
					&& ir.enclosing_region(defining) == home
					&& !seen.contains(&defining)
				{
					work.push(defining);
				}
			}
		}
	}
	RowView::Real
}

/// Whether the work the layer `layer` does per row can be skipped for a row without anyone
/// noticing, under the default context.
#[cfg(test)]
pub(crate) fn layer_work_unobservable(ir: &Ir, facts: &mut FactCache, layer: OpId) -> bool {
	layer_work_unobservable_with(ir, facts, &default_cx(ir), layer)
}

/// Whether the work the layer `layer` does per row can be skipped for a row without anyone
/// noticing, under `cx`.
///
/// True only when the catalog shows every guard of the layer's fields and the transitive answer
/// of the layer's regions is infallible with effects that read the row, stored data or the catalog
/// and nothing else. Answers `false` unless the catalog states every field guard of the layer.
pub(crate) fn layer_work_unobservable_with(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	layer: OpId,
) -> bool {
	guards_visible(cx) && layer_effects_unobservable(ir, facts, cx, layer)
}

/// Whether the catalog states every field guard of the layer, so that the guards the layer's
/// regions hold are known to be all of them.
fn guards_visible(_cx: &EffectCx<'_>) -> bool {
	false
}

/// Whether the transitive answer of the regions of `layer` is infallible and has no effect beyond
/// reads of the row, stored data and the catalog. Not a layer, and a dangling id, answer `false`.
pub(super) fn layer_effects_unobservable(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	layer: OpId,
) -> bool {
	let Some(data) = ir.op(layer) else {
		return false;
	};
	if data.kind.tag() != OpTag::Logical(LogTag::Project) {
		return false;
	}
	for region in data.regions.iter().copied() {
		for child in ops_in(ir, region) {
			if !fallibility::of_with(ir, facts, cx, child).is_infallible() {
				return false;
			}
			if !UNOBSERVABLE.contains(effects::summary_with(ir, facts, cx, child).set) {
				return false;
			}
		}
	}
	true
}

#[cfg(test)]
mod tests {
	use smallvec::smallvec;
	use surrealdb_strand::Strand;

	use super::*;

	fn some(fields: &[&str]) -> FieldSet {
		FieldSet::Some(fields.iter().map(|field| Strand::new(*field)).collect())
	}

	#[test]
	fn field_sets_meet_by_a_shared_field_and_an_empty_set_meets_none() {
		assert!(fields_meet(&FieldSet::All, &FieldSet::All));
		assert!(fields_meet(&FieldSet::All, &some(&["a"])));
		assert!(fields_meet(&some(&["a"]), &FieldSet::All));
		assert!(!fields_meet(&FieldSet::All, &FieldSet::Some(smallvec![])));
		assert!(!fields_meet(&FieldSet::Some(smallvec![]), &FieldSet::All));
		assert!(fields_meet(&some(&["a", "b"]), &some(&["b"])));
		assert!(!fields_meet(&some(&["a"]), &some(&["b"])));
	}

	#[test]
	fn an_unnamed_table_meets_every_table() {
		let a = TableName::new("a");
		let b = TableName::new("b");
		assert!(tables_meet(Some(&a), Some(&a)));
		assert!(!tables_meet(Some(&a), Some(&b)));
		assert!(tables_meet(None, Some(&a)));
		assert!(tables_meet(Some(&a), None));
		assert!(tables_meet(None, None));
	}

	#[test]
	fn the_unobservable_effects_are_reads_alone() {
		assert!(UNOBSERVABLE.contains(EffectSet::PURE));
		assert!(UNOBSERVABLE.contains(EffectSet::READS_ROW));
		assert!(UNOBSERVABLE.contains(EffectSet::READS_ROW | EffectSet::READS_CATALOG));
		assert!(!UNOBSERVABLE.contains(EffectSet::ENTROPY));
		assert!(!UNOBSERVABLE.contains(EffectSet::READS_ROW | EffectSet::WRITES_DATA));
	}
}
