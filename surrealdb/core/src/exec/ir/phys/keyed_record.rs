//! The record-id read of a table whose record ids name their components.
//!
//! The key of a record id over such a table is encoded by its components: an int component holds
//! only ints, and a datetime component orders its key by the whole second alone. So a record id a
//! source names is read as the key that reaches every record it admits ([`keyed_record`]):
//!
//! - a literal float or decimal element of an int component that equals an int is read as that int,
//!   whose key it is;
//! - a bound of a range is cut at a datetime element and that element moved one second outwards,
//!   exclusively, so the bound reaches every key of the element's second;
//! - a bound whose int or datetime element has no key the plan knows (an int element that is not an
//!   int, a datetime element that is not a datetime or has no key text) leaves its side open.
//!
//! A range so widened lists records outside the range as written, so the read is judged by `id`
//! compared with each side as written ([`judge`]). A range is rewritten only where each element of
//! both bounds is a literal, the case where both sides as written can be stated; any other record
//! id is read as written.

use std::ops::Bound;

use smallvec::{SmallVec, smallvec};
use surrealdb_strand::Strand;

use super::index_answer;
use crate::catalog::{ComponentKind, KeyOrigin, KeyStructure};
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, KeyBound, KeyRange, KeyShape, RecordIdAttrs};
use crate::exec::ir::expr::types::result_type;
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::ids::{FrameId, OpId, SnapshotId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::expr::BinaryOperator;
use crate::val::{Array, Datetime, RecordId, RecordIdKey, Value};

/// One element of the key a keyed read names.
#[derive(Clone, Debug)]
enum Element {
	/// The value the record id held at this place.
	Held(ValueId),
	/// A literal the read names in place of the value held.
	Literal(Value),
}

/// The record id a source over a keyed table reads, and the comparisons of `id` each record it
/// lists must meet.
#[derive(Clone, Debug)]
pub(super) struct KeyedRecord {
	table: surrealdb_strand::TableName,
	shape: KeyShape,
	elements: Vec<Element>,
	/// Each comparison is `id <op> <record id>`.
	residual: Vec<(BinaryOperator, Value)>,
}

/// The read of the record id `record`, a source's operand, over a table whose record ids have the
/// structure `key`; `None` where the record id is read as written: a structure that does not come
/// from an `id` field, a key that is not an array or a range of arrays, a key no rule changes, and
/// a range whose elements are not all literals.
pub(super) fn keyed_record(ir: &Ir, record: ValueId, key: &KeyStructure) -> Option<KeyedRecord> {
	if key.origin != KeyOrigin::IdField {
		return None;
	}
	let (attrs, operands) = record_op(ir, record)?;
	match &attrs.key_shape {
		KeyShape::Array {
			..
		} => {
			let (elements, changed) = normalised(ir, key, &operands);
			changed.then(|| KeyedRecord {
				table: attrs.table.clone(),
				shape: attrs.key_shape.clone(),
				elements,
				residual: Vec::new(),
			})
		}
		KeyShape::Range(range) => keyed_range(ir, &attrs.table, key, range, &operands),
		KeyShape::Number(_)
		| KeyShape::String(_)
		| KeyShape::Uuid(_)
		| KeyShape::Generate(_)
		| KeyShape::Object {
			..
		} => None,
	}
}

/// The attributes and operands of the record-id op that defines `record`.
fn record_op(ir: &Ir, record: ValueId) -> Option<(RecordIdAttrs, Vec<ValueId>)> {
	let crate::exec::ir::core::value::ValueDef::OpResult {
		op,
		..
	} = ir.value(record)?.def
	else {
		return None;
	};
	let data = ir.op(op)?;
	match &data.kind {
		OpKind::Expr(ExprOp::RecordId(attrs)) => Some(((**attrs).clone(), data.operands.to_vec())),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

/// The elements `operands` of an array key, each literal float or decimal at an int component
/// that equals an int read as that int, and whether any was.
fn normalised(ir: &Ir, key: &KeyStructure, operands: &[ValueId]) -> (Vec<Element>, bool) {
	let mut changed = false;
	let elements = operands
		.iter()
		.enumerate()
		.map(|(position, operand)| {
			let int = key
				.components
				.get(position)
				.is_some_and(|component| component.kind == ComponentKind::Int);
			if int
				&& let Some(Value::Number(number)) = index_answer::literal(ir, *operand)
				&& !matches!(number, crate::val::Number::Int(_))
				&& let Some(int) = number.as_int_lossless()
			{
				changed = true;
				return Element::Literal(Value::Number(crate::val::Number::Int(int)));
			}
			Element::Held(*operand)
		})
		.collect();
	(elements, changed)
}

/// How many operands a bound of a key range takes.
fn bound_operands(bound: &Bound<KeyBound>) -> usize {
	match bound {
		Bound::Included(key) | Bound::Excluded(key) => match key {
			KeyBound::Array {
				len,
			} => *len as usize,
			KeyBound::Object {
				keys,
			} => keys.len(),
			KeyBound::Number(_)
			| KeyBound::String(_)
			| KeyBound::Uuid(_)
			| KeyBound::Generate(_) => 0,
		},
		Bound::Unbounded => 0,
	}
}

/// Which side of a range a bound is.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
	Low,
	High,
}

/// The read of the key range `range` of `table`, whose bounds take `operands`, start before end.
fn keyed_range(
	ir: &Ir,
	table: &surrealdb_strand::TableName,
	key: &KeyStructure,
	range: &KeyRange,
	operands: &[ValueId],
) -> Option<KeyedRecord> {
	let split = bound_operands(&range.start);
	if split + bound_operands(&range.end) != operands.len() {
		return None;
	}
	let (low, high) = operands.split_at(split);
	let literals = |values: &[ValueId]| -> Option<Vec<Value>> {
		values.iter().map(|value| index_answer::literal(ir, *value)).collect()
	};
	let written_low = literals(low)?;
	let written_high = literals(high)?;
	let (start, start_elements, start_widened, start_changed) =
		widened(key, &range.start, ir, low, Side::Low);
	let (end, end_elements, end_widened, end_changed) =
		widened(key, &range.end, ir, high, Side::High);
	if !(start_widened || end_widened || start_changed || end_changed) {
		return None;
	}
	let mut residual = Vec::new();
	if start_widened || end_widened {
		let side = |bound: &Bound<KeyBound>, written: Vec<Value>, side: Side| {
			let op = match (bound, side) {
				(Bound::Included(_), Side::Low) => BinaryOperator::MoreThanEqual,
				(Bound::Excluded(_), Side::Low) => BinaryOperator::MoreThan,
				(Bound::Included(_), Side::High) => BinaryOperator::LessThanEqual,
				(Bound::Excluded(_), Side::High) => BinaryOperator::LessThan,
				(Bound::Unbounded, _) => return None,
			};
			let key = written_key(bound, written)?;
			Some((
				op,
				Value::RecordId(RecordId {
					table: table.clone(),
					key,
				}),
			))
		};
		residual.extend(side(&range.start, written_low, Side::Low));
		residual.extend(side(&range.end, written_high, Side::High));
		if residual.len()
			!= usize::from(!matches!(range.start, Bound::Unbounded))
				+ usize::from(!matches!(range.end, Bound::Unbounded))
		{
			return None;
		}
	}
	let mut elements = start_elements;
	elements.extend(end_elements);
	Some(KeyedRecord {
		table: table.clone(),
		shape: KeyShape::Range(Box::new(KeyRange {
			start,
			end,
		})),
		elements,
		residual,
	})
}

/// The record id key `bound` names with the literal elements `written`.
fn written_key(bound: &Bound<KeyBound>, written: Vec<Value>) -> Option<RecordIdKey> {
	match bound {
		Bound::Included(key) | Bound::Excluded(key) => match key {
			KeyBound::Number(number) => Some(RecordIdKey::Number(*number)),
			KeyBound::String(string) => Some(RecordIdKey::String(string.clone())),
			KeyBound::Uuid(uuid) => Some(RecordIdKey::Uuid(*uuid)),
			KeyBound::Array {
				..
			} => Some(RecordIdKey::Array(Array(written))),
			KeyBound::Object {
				..
			}
			| KeyBound::Generate(_) => None,
		},
		Bound::Unbounded => None,
	}
}

/// The bound `bound` of a range over a table with key structure `key`, whose elements are
/// `operands`, read as a key that reaches every record it admits: the bound, its elements, whether
/// it was widened, and whether an element was normalised ([`normalised`]).
fn widened(
	key: &KeyStructure,
	bound: &Bound<KeyBound>,
	ir: &Ir,
	operands: &[ValueId],
	side: Side,
) -> (Bound<KeyBound>, Vec<Element>, bool, bool) {
	let (len, included) = match bound {
		Bound::Included(KeyBound::Array {
			len,
		}) => (*len, true),
		Bound::Excluded(KeyBound::Array {
			len,
		}) => (*len, false),
		other @ (Bound::Included(_) | Bound::Excluded(_) | Bound::Unbounded) => {
			return (
				other.clone(),
				operands.iter().copied().map(Element::Held).collect(),
				false,
				false,
			);
		}
	};
	let (elements, changed) = normalised(ir, key, operands);
	for (position, element) in elements.iter().enumerate() {
		let literal = match element {
			Element::Literal(value) => Some(value.clone()),
			Element::Held(value) => index_answer::literal(ir, *value),
		};
		match key.components.get(position).map(|component| &component.kind) {
			Some(ComponentKind::Datetime) => {
				let Some(Value::Datetime(datetime)) = literal else {
					return (Bound::Unbounded, Vec::new(), true, changed);
				};
				let step = chrono::TimeDelta::seconds(match side {
					Side::Low => -1,
					Side::High => 1,
				});
				let moved = datetime
					.floor_second()
					.checked_add_signed(step)
					.map(Datetime)
					.filter(|moved| datetime.in_key_range() && moved.in_key_range());
				let Some(moved) = moved else {
					return (Bound::Unbounded, Vec::new(), true, changed);
				};
				let mut cut = elements[..position].to_vec();
				cut.push(Element::Literal(Value::Datetime(moved)));
				let len = u32::try_from(cut.len()).unwrap_or(u32::MAX);
				return (
					Bound::Excluded(KeyBound::Array {
						len,
					}),
					cut,
					true,
					changed,
				);
			}
			Some(ComponentKind::Int)
				if !matches!(literal, Some(Value::Number(crate::val::Number::Int(_)))) =>
			{
				return (Bound::Unbounded, Vec::new(), true, changed);
			}
			_ => {}
		}
	}
	let shape = KeyBound::Array {
		len,
	};
	let bound = if included {
		Bound::Included(shape)
	} else {
		Bound::Excluded(shape)
	};
	(bound, elements, false, changed)
}

/// Where the ops a keyed read creates are placed, and the frame, snapshot and location they take.
pub(super) struct Site {
	pub(super) before: OpId,
	pub(super) frame: FrameId,
	pub(super) snapshot: SnapshotId,
	pub(super) loc: crate::exec::ir::core::location::Location,
}

impl Site {
	fn expr(&self, kind: ExprOp, operands: SmallVec<[ValueId; 4]>) -> OpSpec {
		let ty = result_type(&kind, &[]);
		OpSpec {
			kind: OpKind::Expr(kind),
			operands,
			results: smallvec![ty],
			frame: self.frame,
			snapshot: self.snapshot,
			loc: self.loc.clone(),
		}
	}
}

/// The record id `read` names, built before `site`.
pub(super) fn record(
	rw: &mut Rewriter<'_>,
	site: &Site,
	read: &KeyedRecord,
) -> Result<ValueId, IrError> {
	let mut operands: SmallVec<[ValueId; 4]> = SmallVec::with_capacity(read.elements.len());
	for element in &read.elements {
		operands.push(match element {
			Element::Held(value) => *value,
			Element::Literal(value) => {
				let op = rw.create(
					InsertPoint::Before(site.before),
					site.expr(ExprOp::Literal(Box::new(value.clone())), SmallVec::new()),
				)?;
				result(rw.ir(), op)?
			}
		});
	}
	let kind = ExprOp::RecordId(Box::new(RecordIdAttrs {
		table: read.table.clone(),
		key_shape: read.shape.clone(),
	}));
	let op = rw.create(InsertPoint::Before(site.before), site.expr(kind, operands))?;
	result(rw.ir(), op)
}

/// The rows `rows` with a filter per comparison of `read`'s residual above them, built before
/// `site`.
pub(super) fn judge(
	rw: &mut Rewriter<'_>,
	site: &Site,
	read: &KeyedRecord,
	mut rows: ValueId,
	rows_type: &TypeKnowledge,
) -> Result<ValueId, IrError> {
	for (op, bound) in &read.residual {
		let filter = rw.create(
			InsertPoint::Before(site.before),
			OpSpec {
				kind: OpKind::Phys(PhysOp::Filter),
				operands: smallvec![rows],
				results: smallvec![rows_type.clone()],
				frame: site.frame,
				snapshot: site.snapshot,
				loc: site.loc.clone(),
			},
		)?;
		let (_, block) = rw.create_region(
			filter,
			RegionKind::Clause(ClauseRole::Where),
			&[TypeKnowledge::Unknown],
		)?;
		let row = rw
			.ir()
			.block(block)
			.and_then(|held| held.args.first().copied())
			.ok_or(IrError::Stale("the row of a where clause"))?;
		let at = InsertPoint::End(block);
		let id = rw.create(
			at,
			site.expr(
				ExprOp::Field {
					name: Strand::from("id"),
				},
				smallvec![row],
			),
		)?;
		let id = result(rw.ir(), id)?;
		let bound =
			rw.create(at, site.expr(ExprOp::Literal(Box::new(bound.clone())), SmallVec::new()))?;
		let bound = result(rw.ir(), bound)?;
		let compared = rw.create(
			at,
			site.expr(
				ExprOp::Binary {
					op: op.clone(),
				},
				smallvec![id, bound],
			),
		)?;
		let compared = result(rw.ir(), compared)?;
		rw.create(
			at,
			OpSpec {
				kind: OpKind::Expr(ExprOp::Yield),
				operands: smallvec![compared],
				results: SmallVec::new(),
				frame: site.frame,
				snapshot: site.snapshot,
				loc: site.loc.clone(),
			},
		)?;
		rows = result(rw.ir(), filter)?;
	}
	Ok(rows)
}

/// Erases the record id `record` the source named once nothing reads it, and then each of its
/// elements nothing reads. A record id and its literal elements have no effect that erasing them
/// would lose.
pub(super) fn retire(rw: &mut Rewriter<'_>, record: ValueId) -> Result<(), IrError> {
	let mut pending = vec![record];
	while let Some(value) = pending.pop() {
		let Some(crate::exec::ir::core::value::ValueDef::OpResult {
			op,
			..
		}) = rw.ir().value(value).map(|data| data.def)
		else {
			continue;
		};
		let Some(data) = rw.ir().op(op) else {
			continue;
		};
		let pure = matches!(data.kind, OpKind::Expr(ExprOp::RecordId(_) | ExprOp::Literal(_)));
		if !pure || data.results.iter().any(|result| !rw.ir().uses(*result).is_empty()) {
			continue;
		}
		let operands = data.operands.clone();
		match rw.erase(op, EliminationReason::DeadPure) {
			Ok(()) => pending.extend(operands),
			Err(IrError::HasUses(_)) => {}
			Err(error) => return Err(error),
		}
	}
	Ok(())
}

/// The first result of `op`.
fn result(ir: &Ir, op: OpId) -> Result<ValueId, IrError> {
	ir.op(op).and_then(|data| data.results.first().copied()).ok_or(IrError::Stale("result"))
}
