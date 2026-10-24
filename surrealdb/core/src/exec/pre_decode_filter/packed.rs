//! The pre-decode filter over a packed row's columns.
//!
//! A packed row that has passed its check ([`CheckedPackedRow`]) is judged where its columns lie,
//! as a record in the stored-value encoding is judged by [`PreDecodeFilter::eval_stored`]: each
//! leaf is read alone, as the value the whole decode holds at its path, and judged by the
//! comparison the authoritative evaluator makes of it. A leaf whose path steps where the stored
//! encoding's descent would not step is unknown, which keeps the row: the path of a leaf is
//! followed exactly as [`descend_stored`] follows it through the same record encoded as a
//! document.

use super::{
	Evidence, FusedFlatClauses, PathSegment, PreDecodeFilter, PredNode, combine_and, combine_or,
};
use crate::catalog::packed::{CheckedPackedRow, FieldRef, PathRead};
use crate::exec::object_extract::{DescendResult, Extracted};
use crate::val::stored::{self, ValueRef};
use crate::val::{Bytes, Datetime, Duration, Geometry, Number, RecordId, Uuid, Value};

impl PreDecodeFilter {
	/// [`Self::eval_stored`] over the columns of the packed row `row`: each leaf is the value the
	/// whole decode of the row holds at its path, and a leaf that cannot be read as such is
	/// unknown.
	pub(super) fn eval_packed(
		&self,
		row: CheckedPackedRow<'_>,
		prefix: &[&PathSegment],
		node: &PredNode,
	) -> Evidence {
		match node {
			PredNode::And(xs) => combine_and(xs.iter().map(|x| self.eval_packed(row, prefix, x))),
			PredNode::Or(xs) => combine_or(xs.iter().map(|x| self.eval_packed(row, prefix, x))),
			PredNode::Not(inner) => match self.eval_packed(row, prefix, inner) {
				Evidence::ProvablyFalse => Evidence::ProvablyTrue,
				Evidence::ProvablyTrue => Evidence::ProvablyFalse,
				Evidence::Unknown => Evidence::Unknown,
			},
			PredNode::Leaf {
				path,
				op,
				literal,
				reversed,
				..
			} => match self.packed_leaf(row, prefix, path) {
				DescendResult::Found(Some(v)) => {
					super::evidence_from_binary_cmp(op, literal, *reversed, &v)
				}
				DescendResult::Missing => {
					super::evidence_from_binary_cmp(op, literal, *reversed, &Value::None)
				}
				DescendResult::Found(None) | DescendResult::Bail => Evidence::Unknown,
			},
			PredNode::LeafSetMembership {
				path,
				op,
				set,
				literal_set,
				..
			} => match self.packed_leaf(row, prefix, path) {
				DescendResult::Found(Some(v)) => {
					super::hash_set_evidence_for(op, set.contains(&v), &v, literal_set)
				}
				// Field absent → not in set.
				DescendResult::Missing => Self::set_evidence_for(op, false),
				DescendResult::Found(None) | DescendResult::Bail => Evidence::Unknown,
			},
			PredNode::LeafStreaming {
				path,
				evaluator,
				fallback,
			} => match self.packed_leaf(row, prefix, path) {
				DescendResult::Found(Some(v)) => evaluator.evaluate_decoded(&v, fallback),
				DescendResult::Missing => evaluator.evaluate_missing(fallback),
				DescendResult::Found(None) | DescendResult::Bail => Evidence::Unknown,
			},
			PredNode::FusedFlatMapAnd {
				clauses,
				..
			} => self.eval_packed_fused(row, prefix, clauses),
			PredNode::NavigatePrefix {
				segment,
				child,
			} => {
				let mut next: Vec<&PathSegment> = Vec::with_capacity(prefix.len() + 1);
				next.extend_from_slice(prefix);
				next.push(segment);
				self.eval_packed(row, &next, child.as_ref())
			}
		}
	}

	/// The leaf `prefix ++ path` names in a packed row, as the whole decode holds it.
	fn packed_leaf(
		&self,
		row: CheckedPackedRow<'_>,
		prefix: &[&PathSegment],
		path: &[PathSegment],
	) -> DescendResult<Option<Value>> {
		let segments = prefix.iter().copied().chain(path.iter()).map(PathSegment::as_str);
		leaf_of(row, segments, self.depth_limit)
	}

	/// [`Self::eval_stored_fused`] over a packed row: each clause's field of the object `prefix`
	/// names, read once for all of the clause's ops.
	fn eval_packed_fused(
		&self,
		row: CheckedPackedRow<'_>,
		prefix: &[&PathSegment],
		clauses: &FusedFlatClauses,
	) -> Evidence {
		if clauses.is_empty() {
			return Evidence::Unknown;
		}
		combine_and(clauses.iter().flat_map(|clause| {
			// An absent field is NONE; one that cannot be read as the decode holds it is unknown.
			let value = match std::str::from_utf8(&clause.key_utf8) {
				Ok(key) => {
					let segments =
						prefix.iter().map(|segment| segment.as_str()).chain(std::iter::once(key));
					match leaf_of(row, segments, self.depth_limit) {
						DescendResult::Found(value) => value,
						DescendResult::Missing => Some(Value::None),
						DescendResult::Bail => None,
					}
				}
				Err(_) => None,
			};
			clause.ops.iter().map(move |opc| match &value {
				Some(v) => super::evidence_from_binary_cmp(&opc.op, &opc.literal, opc.reversed, v),
				None => Evidence::Unknown,
			})
		}))
	}
}

/// The value at `path` of the packed row `row`, as the whole decode holds it, and as
/// [`extract_field_from_record_bytes`](crate::exec::object_extract::extract_field_from_record_bytes)
/// reaches it in the same record encoded as a document: [`Extracted::Missing`] where a key on the
/// path is absent, and [`Extracted::Bail`] where the path steps through no object or its leaf is
/// not read.
pub(crate) fn extract_from_packed(
	row: CheckedPackedRow<'_>,
	path: &[PathSegment],
	depth_limit: u32,
) -> Extracted {
	match leaf_of(row, path.iter().map(PathSegment::as_str), depth_limit) {
		DescendResult::Found(Some(value)) => Extracted::Found(value),
		DescendResult::Missing => Extracted::Missing,
		DescendResult::Found(None) | DescendResult::Bail => Extracted::Bail,
	}
}

/// The value at the path `segments` of the packed row `row`, as the whole decode holds it, and as
/// [`descend_stored`] reaches it in the same record encoded as a document: `Missing` where a key
/// on the path is absent from the object that should hold it, `Bail` where a step goes through
/// something that is not an object, or a read the row cannot answer, and `Found(None)` for a leaf
/// that does not decode.
///
/// The path is walked once, top down ([`CheckedPackedRow::read_path`]): an object node, or an
/// object a stored value holds, is stepped into; a key that is absent ends the path as missing, and
/// a step past any other value, `NULL` among them, ends it as a step through no object. A path that
/// opens with `id` is not in the row.
fn leaf_of<'p>(
	row: CheckedPackedRow<'_>,
	segments: impl Iterator<Item = &'p str> + Clone,
	depth_limit: u32,
) -> DescendResult<Option<Value>> {
	let total = segments.clone().count();
	if total == 0 || total > depth_limit as usize || segments.clone().next() == Some("id") {
		return DescendResult::Bail;
	}
	match row.read_path(segments.clone()) {
		Ok(PathRead::Missing) => DescendResult::Missing,
		// A step past a scalar, a literal or a NULL, or a read the row cannot answer, is no step
		// the stored encoding takes.
		Ok(PathRead::NotAnObject) | Err(_) => DescendResult::Bail,
		Ok(PathRead::Held {
			value,
			depth,
		}) => match descend_value(value, segments.skip(depth)) {
			DescendResult::Found(found) => DescendResult::Found(found.decode().ok()),
			DescendResult::Missing => DescendResult::Missing,
			DescendResult::Bail => DescendResult::Bail,
		},
		Ok(PathRead::Field {
			field,
			column,
		}) => DescendResult::Found(value_of(row, field, column, total)),
	}
}

/// The value reached by stepping through `rest` from `value`, a value of a checked row, as
/// [`descend_stored`] steps through an object's keys. The row's check validated the value, so every
/// object in it holds its keys in order.
fn descend_value<'a, 'p>(
	mut value: ValueRef<'a>,
	rest: impl Iterator<Item = &'p str>,
) -> DescendResult<ValueRef<'a>> {
	for key in rest {
		let Ok(Some(object)) = value.as_object() else {
			return DescendResult::Bail;
		};
		value = match object.validated().get(key) {
			Ok(Some(next)) => next,
			Ok(None) => return DescendResult::Missing,
			Err(_) => return DescendResult::Bail,
		};
	}
	DescendResult::Found(value)
}

/// The value the whole decode holds for `field`, the read of a path of `total` steps from
/// `column` when the row holds it in one, or `None` where it cannot be built.
fn value_of(
	row: CheckedPackedRow<'_>,
	field: FieldRef<'_>,
	column: Option<usize>,
	total: usize,
) -> Option<Value> {
	Some(match field {
		FieldRef::Absent => Value::None,
		FieldRef::Null => Value::Null,
		FieldRef::Object => Value::Object(row.decode_node(column?).ok()?),
		FieldRef::Bool(v) => Value::Bool(v),
		FieldRef::I8(v) => Value::Number(Number::I8(v)),
		FieldRef::I16(v) => Value::Number(Number::I16(v)),
		FieldRef::I32(v) => Value::Number(Number::I32(v)),
		FieldRef::Int(v) => Value::Number(Number::Int(v)),
		FieldRef::U8(v) => Value::Number(Number::U8(v)),
		FieldRef::U16(v) => Value::Number(Number::U16(v)),
		FieldRef::U32(v) => Value::Number(Number::U32(v)),
		FieldRef::U64(v) => Value::Number(Number::U64(v)),
		FieldRef::F16(v) => Value::Number(Number::F16(v)),
		FieldRef::F32(v) => Value::Number(Number::F32(v)),
		FieldRef::Float(v) => Value::Number(Number::Float(v)),
		FieldRef::Decimal(v) => Value::Number(Number::Decimal(v)),
		FieldRef::Uuid(v) => Value::Uuid(Uuid(v)),
		FieldRef::Datetime(v) => Value::Datetime(Datetime(v)),
		FieldRef::Duration(v) => Value::Duration(Duration(v)),
		FieldRef::Point(x, y) => Value::Geometry(Geometry::Point((x, y).into())),
		FieldRef::String(v) => Value::String(v.into()),
		FieldRef::Bytes(v) => Value::Bytes(Bytes::from(v.to_vec())),
		FieldRef::Literal(v) => v.clone(),
		// A record id sits one level below the path to it.
		FieldRef::RecordId(table, key) => Value::RecordId(RecordId {
			table: table.clone(),
			key: stored::decode_key_nested(key, total + 1).ok()?,
		}),
		FieldRef::Stored(v) => v.decode().ok()?,
	})
}
