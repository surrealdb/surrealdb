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
//!
//! A packed row is judged from its columns only after a check that walks the whole row, which a
//! row the predicate keeps repays with nothing: it is then decoded by a second walk. The pass
//! therefore states the shape as [`PreDecodeShape::Selective`] where the predicate refuses most
//! of the rows it judges ([`refuses_most`]), and the scan checks its packed rows for it only
//! then. A shape that is not selective judges documents the same, and leaves each packed row to
//! the decode. A range comparison is never selective, since the catalog holds no statistics of a
//! field's values: a packed scan whose only filter is a range decodes every row it reads, however
//! few the range keeps.

#![deny(clippy::wildcard_enum_match_arm)]

use std::sync::Arc;

use super::access::ReadVersion;
use super::conjunct::{self, def_op, strip_truthy};
use super::ops::PhysOp;
use crate::catalog::FieldDefinition;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::Rewriter;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ShortCircuitOp};
use crate::exec::ir::iface::catalog::{CatalogAnswer, PhysCx, SourceKey};
use crate::exec::ir::iface::frame::Snapshot;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::logical::ops::LoopKind;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::object_extract::PathSegment;
use crate::exec::pre_decode_filter::PreDecodeShape;
use crate::expr::{BinaryOperator, Kind, Part, PrefixOperator};
use crate::val::{Number, Value};

/// States the prefilter of every scan of `ir` and answers the number of scans whose attribute
/// changed.
///
/// A table's key structure is read from the physical catalog's answers for the scan's own read
/// (the table, its `WITH` hint, and the current snapshot or a stamp only the run evaluates). A
/// table the catalog does not hold, or answers as unknown, gives its scan no prefilter: the scan
/// then decodes every row, as the predicate judges it.
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
	let (key, fields) = match &data.kind {
		OpKind::Phys(PhysOp::TableScan(attrs)) => {
			if attrs.lightweight {
				return None;
			}
			// A read at a stamp only the run evaluates is answered under its own key; the key
			// structure it answers is the table's.
			let version = match ir.snapshot(data.snapshot) {
				Some(Snapshot::Version(_)) => ReadVersion::Deferred,
				Some(Snapshot::Current) | None => ReadVersion::Current,
			};
			let answers: SourceKey = (attrs.table.clone(), attrs.read.with.clone(), version);
			match cx.physical.table_answers(&answers) {
				CatalogAnswer::Found(found) => (found.key.clone(), Arc::clone(&found.fields)),
				CatalogAnswer::NotLoaded | CatalogAnswer::Unknown | CatalogAnswer::Absent => {
					return None;
				}
			}
		}
		OpKind::Phys(PhysOp::KeyRangeScan(attrs)) => {
			let answers: SourceKey =
				(attrs.table.clone(), attrs.read.with.clone(), ReadVersion::Current);
			let fields = match cx.physical.table_answers(&answers) {
				CatalogAnswer::Found(found) => Arc::clone(&found.fields),
				CatalogAnswer::NotLoaded | CatalogAnswer::Unknown | CatalogAnswer::Absent => {
					Arc::from(Vec::new())
				}
			};
			(Some(Arc::clone(&attrs.access.key)), fields)
		}
		OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_) => return None,
	};
	let (filter, row) = predicate_region(ir, scan)?;
	let site = conjunct::site(ir, filter)?;
	if site.once {
		return None;
	}
	let shape = shape_of(ir, row, site.root)?;
	if key.as_deref().is_some_and(|key| shape.reads_key_component(key)) {
		return None;
	}
	Some(if refuses_most(&shape, false, &fields) {
		PreDecodeShape::Selective(Box::new(shape))
	} else {
		shape
	})
}

/// Whether the predicate of `shape` (of its negation, when `negated`) is one that refuses most of
/// the rows it judges, over rows of a table whose fields are `fields`.
///
/// The catalog holds no statistics of a field's values, so this reads the comparison alone, as a
/// planner does without them ([`Refusal`]). A conjunction refuses most where any conjunct does,
/// and a disjunction only where every disjunct does; a negation swaps the two. A comparison whose
/// refusal is not known refuses most in neither polarity.
fn refuses_most(shape: &PreDecodeShape, negated: bool, fields: &[FieldDefinition]) -> bool {
	match shape {
		PreDecodeShape::And(parts) if !negated => {
			parts.iter().any(|part| refuses_most(part, false, fields))
		}
		PreDecodeShape::And(parts) => parts.iter().all(|part| refuses_most(part, true, fields)),
		PreDecodeShape::Or(parts) if !negated => {
			parts.iter().all(|part| refuses_most(part, false, fields))
		}
		PreDecodeShape::Or(parts) => parts.iter().any(|part| refuses_most(part, true, fields)),
		PreDecodeShape::Not(inner) => refuses_most(inner, !negated, fields),
		PreDecodeShape::Selective(inner) => refuses_most(inner, negated, fields),
		PreDecodeShape::Leaf {
			path,
			op,
			literal,
			reversed,
		} => match refusal(path, op, literal, *reversed, fields) {
			Refusal::Most => !negated,
			Refusal::Least => negated,
			Refusal::Unknown => false,
		},
	}
}

/// How many of the rows it judges a comparison refuses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refusal {
	/// Most: an equality, a containment, a membership.
	Most,
	/// The fewest: an inequality, a negated containment or membership.
	Least,
	/// Not known without statistics: a range, a membership that the filter cannot decide for
	/// the numbers the field holds, and an operator that is no comparison.
	Unknown,
}

/// The refusal of the comparison `path <op> literal` (`literal <op> path` when `reversed`) over
/// rows of a table whose fields are `fields`.
///
/// A comparison against a set that holds a number refuses a row whose field is a number only where
/// the filter decides a miss of it, which it does for an integer against integers alone
/// ([`hash_miss_is_decisive`](crate::exec::pre_decode_filter)). Such a membership is as its
/// operator says where the field is declared an integer and `Unknown` otherwise: a field of
/// another number kind, or of none declared, is never refused. A containment of numbers is
/// `Unknown` whatever the field is declared, since the field is a collection.
fn refusal(
	path: &[PathSegment],
	op: &BinaryOperator,
	literal: &Value,
	reversed: bool,
	fields: &[FieldDefinition],
) -> Refusal {
	let by_operator = match op {
		BinaryOperator::NotEqual
		| BinaryOperator::NotContain
		| BinaryOperator::ContainNone
		| BinaryOperator::NotInside
		| BinaryOperator::NoneInside => Refusal::Least,
		BinaryOperator::LessThan
		| BinaryOperator::LessThanEqual
		| BinaryOperator::MoreThan
		| BinaryOperator::MoreThanEqual => return Refusal::Unknown,
		// No comparison: nothing is known of the rows it keeps.
		BinaryOperator::Subtract
		| BinaryOperator::Add
		| BinaryOperator::Multiply
		| BinaryOperator::Divide
		| BinaryOperator::Remainder
		| BinaryOperator::Power
		| BinaryOperator::Or
		| BinaryOperator::And
		| BinaryOperator::NullCoalescing
		| BinaryOperator::TenaryCondition
		| BinaryOperator::Range
		| BinaryOperator::RangeInclusive
		| BinaryOperator::RangeSkip
		| BinaryOperator::RangeSkipInclusive => return Refusal::Unknown,
		BinaryOperator::Equal
		| BinaryOperator::ExactEqual
		| BinaryOperator::AllEqual
		| BinaryOperator::AnyEqual
		| BinaryOperator::Contain
		| BinaryOperator::ContainAll
		| BinaryOperator::ContainAny
		| BinaryOperator::Inside
		| BinaryOperator::AllInside
		| BinaryOperator::AnyInside
		| BinaryOperator::Outside
		| BinaryOperator::Intersects
		| BinaryOperator::Matches(_)
		| BinaryOperator::NearestNeighbor(_) => Refusal::Most,
	};
	let in_set = matches!(op, BinaryOperator::Inside | BinaryOperator::NotInside) && !reversed;
	let against_set = in_set
		|| matches!(
			op,
			BinaryOperator::AllInside
				| BinaryOperator::AnyInside
				| BinaryOperator::NoneInside
				| BinaryOperator::ContainAll
				| BinaryOperator::ContainAny
				| BinaryOperator::ContainNone
		);
	if !against_set {
		return by_operator;
	}
	match numbers_of(literal) {
		Numbers::None => by_operator,
		Numbers::Ints if in_set && declares_int(fields, path) => by_operator,
		Numbers::Ints | Numbers::Inexact => Refusal::Unknown,
	}
}

/// The numbers a membership's set holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Numbers {
	/// None at its top level.
	None,
	/// Integers, and no other kind of number.
	Ints,
	/// A `Float` or a `Decimal`.
	Inexact,
}

/// The numbers the collection `set` holds as elements.
#[allow(clippy::wildcard_enum_match_arm)]
fn numbers_of(set: &Value) -> Numbers {
	match set {
		Value::Array(array) => numbers_among(array.iter()),
		Value::Set(set) => numbers_among(set.iter()),
		_ => Numbers::None,
	}
}

/// The numbers among `elements`.
#[allow(clippy::wildcard_enum_match_arm)]
fn numbers_among<'a>(elements: impl Iterator<Item = &'a Value>) -> Numbers {
	let mut found = Numbers::None;
	for element in elements {
		match element {
			Value::Number(Number::Float(_) | Number::Decimal(_)) => return Numbers::Inexact,
			Value::Number(Number::Int(_)) => found = Numbers::Ints,
			_ => {}
		}
	}
	found
}

/// Whether the table declares the field `path` names as an integer, or as a union of `none` and
/// `int` that holds the integer: the field whose name is the path of object keys `path` holds, at
/// the root or nested, and that is not computed.
#[allow(clippy::wildcard_enum_match_arm)]
fn declares_int(fields: &[FieldDefinition], path: &[PathSegment]) -> bool {
	if path.is_empty() {
		return false;
	}
	let int_valued = |kind: &Kind| match kind {
		Kind::Int => true,
		Kind::Either(kinds) => {
			kinds.iter().any(|kind| matches!(kind, Kind::Int))
				&& kinds.iter().all(|kind| matches!(kind, Kind::Int | Kind::None))
		}
		_ => false,
	};
	let named = |field: &FieldDefinition| {
		field.name.0.len() == path.len()
			&& field.name.0.iter().zip(path).all(
				|(part, segment)| matches!(part, Part::Field(name) if name.as_str() == segment.as_str()),
			)
	};
	fields.iter().any(|field| {
		named(field)
			&& field.computed.is_none()
			&& field.field_kind.as_ref().is_some_and(int_valued)
	})
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

#[cfg(test)]
mod tests {
	use super::*;

	fn int(v: i64) -> Value {
		Value::Number(Number::Int(v))
	}

	fn field(name: &str, kind: Kind) -> FieldDefinition {
		FieldDefinition {
			name: crate::expr::Idiom::field(name),
			field_kind: Some(kind),
			..Default::default()
		}
	}

	fn leaf(op: BinaryOperator, literal: Value) -> PreDecodeShape {
		PreDecodeShape::Leaf {
			path: vec![PathSegment::from("a")].into(),
			op,
			literal,
			reversed: false,
		}
	}

	/// Whether `shape` refuses most rows of a table that declares the integer field `a`.
	fn selective(shape: &PreDecodeShape) -> bool {
		refuses_most(shape, false, &[field("a", Kind::Int)])
	}

	#[test]
	fn an_equality_and_a_membership_refuse_most_rows() {
		for op in [
			BinaryOperator::Equal,
			BinaryOperator::ExactEqual,
			BinaryOperator::Contain,
			BinaryOperator::ContainAny,
		] {
			assert!(selective(&leaf(op.clone(), int(1))), "{op:?}");
		}
		let strings = Value::from(vec![Value::from("x"), Value::from("y")]);
		assert!(selective(&leaf(BinaryOperator::Inside, strings)));
	}

	#[test]
	fn an_inequality_and_a_negated_membership_keep_most_rows() {
		for op in [
			BinaryOperator::NotEqual,
			BinaryOperator::NotContain,
			BinaryOperator::ContainNone,
			BinaryOperator::NotInside,
			BinaryOperator::NoneInside,
		] {
			assert!(!selective(&leaf(op.clone(), int(1))), "{op:?}");
		}
	}

	#[test]
	fn a_range_refuses_most_rows_in_neither_polarity() {
		for op in [
			BinaryOperator::LessThan,
			BinaryOperator::LessThanEqual,
			BinaryOperator::MoreThan,
			BinaryOperator::MoreThanEqual,
		] {
			let range = leaf(op.clone(), int(1));
			assert!(!selective(&range), "{op:?}");
			assert!(!selective(&PreDecodeShape::Not(Box::new(range))), "NOT {op:?}");
		}
	}

	#[test]
	fn a_containment_of_integers_refuses_most_rows_in_neither_polarity() {
		let ints = || Value::from(vec![int(1), int(2)]);
		for op in [BinaryOperator::ContainAny, BinaryOperator::AnyInside, BinaryOperator::NotInside]
		{
			let shape = leaf(op.clone(), ints());
			let negated = PreDecodeShape::Not(Box::new(leaf(op.clone(), ints())));
			let by_integer = matches!(op, BinaryOperator::NotInside);
			// Only a plain membership of a declared integer field is decided.
			assert!(!selective(&shape), "{op:?}");
			assert_eq!(selective(&negated), by_integer, "NOT {op:?}");
		}
	}

	#[test]
	fn a_membership_of_integers_refuses_most_rows_of_a_declared_integer_field_only() {
		let ints = || Value::from(vec![int(1), int(2)]);
		let member = || leaf(BinaryOperator::Inside, ints());
		assert!(selective(&member()));
		let optional = Kind::Either(vec![Kind::None, Kind::Int]);
		assert!(refuses_most(&member(), false, &[field("a", optional)]));
		// A union that holds no integer declares none.
		assert!(!refuses_most(&member(), false, &[field("a", Kind::Either(vec![Kind::None]))]));
		assert!(!refuses_most(&member(), false, &[field("a", Kind::Either(vec![]))]));
		for kind in [Kind::Float, Kind::Decimal, Kind::Number, Kind::Any] {
			assert!(!refuses_most(&member(), false, &[field("a", kind.clone())]), "{kind:?}");
		}
		// No declaration, another field, a computed one and a nested path declare no integer.
		assert!(!refuses_most(&member(), false, &[]));
		assert!(!refuses_most(&member(), false, &[field("b", Kind::Int)]));
		let mut computed = field("a", Kind::Int);
		computed.computed = Some(crate::expr::Expr::Literal(crate::expr::Literal::None));
		assert!(!refuses_most(&member(), false, &[computed]));
		let nested = PreDecodeShape::Leaf {
			path: vec![PathSegment::from("a"), PathSegment::from("b")].into(),
			op: BinaryOperator::Inside,
			literal: ints(),
			reversed: false,
		};
		assert!(!refuses_most(&nested, false, &[field("a", Kind::Int)]));
	}

	/// A nested field is declared by the definition of its whole path.
	#[test]
	fn a_membership_of_integers_refuses_most_rows_of_a_declared_nested_integer_field() {
		let nested = |literal: Value| PreDecodeShape::Leaf {
			path: vec![PathSegment::from("a"), PathSegment::from("b")].into(),
			op: BinaryOperator::Inside,
			literal,
			reversed: false,
		};
		let ints = || Value::from(vec![int(1), int(2)]);
		let declared = |kind: Kind| FieldDefinition {
			name: crate::expr::Idiom(vec![Part::Field("a".into()), Part::Field("b".into())]),
			field_kind: Some(kind),
			..Default::default()
		};
		assert!(refuses_most(&nested(ints()), false, &[declared(Kind::Int)]));
		// Another number kind, a float in the set, a computed field and the declaration of the
		// parent alone decide nothing.
		assert!(!refuses_most(&nested(ints()), false, &[declared(Kind::Float)]));
		let mixed = Value::from(vec![int(1), Value::Number(Number::Float(1.5))]);
		assert!(!refuses_most(&nested(mixed), false, &[declared(Kind::Int)]));
		let mut computed = declared(Kind::Int);
		computed.computed = Some(crate::expr::Expr::Literal(crate::expr::Literal::None));
		assert!(!refuses_most(&nested(ints()), false, &[computed]));
		assert!(!refuses_most(&nested(ints()), false, &[field("a", Kind::Int)]));
		// A field of the path's last segment at the root is not the nested field.
		assert!(!refuses_most(&nested(ints()), false, &[field("b", Kind::Int)]));
	}

	#[test]
	fn an_operator_that_is_no_comparison_refuses_most_rows_in_neither_polarity() {
		for op in [
			BinaryOperator::Add,
			BinaryOperator::Subtract,
			BinaryOperator::Multiply,
			BinaryOperator::Divide,
			BinaryOperator::Remainder,
			BinaryOperator::Power,
			BinaryOperator::Or,
			BinaryOperator::And,
			BinaryOperator::NullCoalescing,
			BinaryOperator::TenaryCondition,
			BinaryOperator::Range,
			BinaryOperator::RangeInclusive,
			BinaryOperator::RangeSkip,
			BinaryOperator::RangeSkipInclusive,
		] {
			let shape = leaf(op.clone(), int(1));
			assert!(!selective(&shape), "{op:?}");
			assert!(!selective(&PreDecodeShape::Not(Box::new(shape))), "NOT {op:?}");
		}
	}

	#[test]
	fn a_comparison_against_inexact_numbers_refuses_most_rows_in_neither_polarity() {
		for number in [
			Value::Number(Number::Float(1.5)),
			Value::Number(Number::Decimal(rust_decimal::Decimal::from(2))),
		] {
			for op in [
				BinaryOperator::Inside,
				BinaryOperator::NotInside,
				BinaryOperator::AnyInside,
				BinaryOperator::NoneInside,
				BinaryOperator::ContainAny,
				BinaryOperator::ContainNone,
			] {
				let member = leaf(op.clone(), Value::from(vec![int(1), number.clone()]));
				assert!(!selective(&member), "{op:?}");
				assert!(!selective(&PreDecodeShape::Not(Box::new(member))), "NOT {op:?}");
			}
		}
		// The element of a membership in the field is no set.
		let element = PreDecodeShape::Leaf {
			path: vec![PathSegment::from("a")].into(),
			op: BinaryOperator::Inside,
			literal: Value::Number(Number::Float(1.5)),
			reversed: true,
		};
		assert!(selective(&element));
	}

	#[test]
	fn a_conjunction_refuses_most_where_a_conjunct_does_and_a_disjunction_where_all_do() {
		let eq = || leaf(BinaryOperator::Equal, int(1));
		let ne = || leaf(BinaryOperator::NotEqual, int(1));
		assert!(selective(&PreDecodeShape::And(vec![ne(), eq()])));
		assert!(!selective(&PreDecodeShape::And(vec![ne(), ne()])));
		assert!(selective(&PreDecodeShape::Or(vec![eq(), eq()])));
		assert!(!selective(&PreDecodeShape::Or(vec![eq(), ne()])));
	}

	#[test]
	fn a_negation_swaps_what_it_refuses() {
		let eq = || leaf(BinaryOperator::Equal, int(1));
		let ne = || leaf(BinaryOperator::NotEqual, int(1));
		assert!(selective(&PreDecodeShape::Not(Box::new(ne()))));
		assert!(!selective(&PreDecodeShape::Not(Box::new(eq()))));
		// NOT (a != 1 AND a = 1) is a = 1 OR a != 1: it keeps most.
		assert!(!selective(&PreDecodeShape::Not(Box::new(PreDecodeShape::And(vec![ne(), eq()])))));
		// NOT (a != 1 OR a != 1) is a = 1 AND a = 1.
		assert!(selective(&PreDecodeShape::Not(Box::new(PreDecodeShape::Or(vec![ne(), ne()])))));
	}
}
