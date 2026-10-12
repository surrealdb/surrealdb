//! Strict identity of values.
//!
//! `==` on a [`Value`] is a numeric comparison: `1` equals `1.0f`, `-0.0` equals `0.0`, and a
//! decimal equals another of a different scale. Code that substitutes one value for another (a
//! constant folded into compiled code, a record id left out of stored bytes, a value read back
//! from what was just written) needs the stronger relation here: the two are the *same* value,
//! indistinguishable by any operation.

use std::ops::Bound;

use super::{Number, Object, RecordId, RecordIdKey, Value};

/// Whether `a` and `b` are the same value, not merely equal ones.
///
/// - Both are of one variant throughout.
/// - Numbers are of one kind: ints are equal, floats have the same bits (NaN payload and sign
///   included, so `0.0` and `-0.0` differ), decimals have the same serialized form (scale and sign
///   included).
/// - Array elements, set elements and object entries are the same in the same order.
/// - Record ids have the same table and the same key under [`same_record_key`].
/// - Range bounds are of one kind with endpoints that are the same.
/// - Geometry coordinates have the same bits.
///
/// A value folded into compiled code stands for another only where they are the same: `1` and
/// `1.0f` compare equal, but a body that folded `1` answers `type::of` with `int`.
pub fn same_value(a: &Value, b: &Value) -> bool {
	match (a, b) {
		(Value::Number(x), Value::Number(y)) => same_number(x, y),
		(Value::Array(x), Value::Array(y)) => {
			x.len() == y.len() && x.iter().zip(y.iter()).all(|(x, y)| same_value(x, y))
		}
		(Value::Set(x), Value::Set(y)) => {
			x.len() == y.len() && x.iter().zip(y.iter()).all(|(x, y)| same_value(x, y))
		}
		(Value::Object(x), Value::Object(y)) => same_object(x, y),
		(Value::RecordId(x), Value::RecordId(y)) => same_record_id(x, y),
		(Value::Range(x), Value::Range(y)) => {
			same_bound(&x.start, &y.start, same_value) && same_bound(&x.end, &y.end, same_value)
		}
		(Value::Geometry(x), Value::Geometry(y)) => same_geometry(x, y),
		_ => std::mem::discriminant(a) == std::mem::discriminant(b) && a == b,
	}
}

/// [`same_value`] over two record ids: the same table and the same key under
/// [`same_record_key`].
pub fn same_record_id(a: &RecordId, b: &RecordId) -> bool {
	a.table == b.table && same_record_key(&a.key, &b.key)
}

/// [`same_value`] over two record id keys: of one variant, with numbers of one kind wherever they
/// nest in an array, object or range key.
pub fn same_record_key(a: &RecordIdKey, b: &RecordIdKey) -> bool {
	match (a, b) {
		(RecordIdKey::Number(x), RecordIdKey::Number(y)) => x == y,
		(RecordIdKey::String(x), RecordIdKey::String(y)) => x == y,
		(RecordIdKey::Uuid(x), RecordIdKey::Uuid(y)) => x == y,
		(RecordIdKey::Array(x), RecordIdKey::Array(y)) => {
			x.len() == y.len() && x.iter().zip(y.iter()).all(|(x, y)| same_value(x, y))
		}
		(RecordIdKey::Object(x), RecordIdKey::Object(y)) => same_object(x, y),
		(RecordIdKey::Range(x), RecordIdKey::Range(y)) => {
			same_bound(&x.start, &y.start, same_record_key)
				&& same_bound(&x.end, &y.end, same_record_key)
		}
		_ => false,
	}
}

/// [`same_value`] over two objects, with every entry of `b` named `skip` left out of the
/// comparison: the remaining entries of `b` and all of `a`'s are the same keys in the same order,
/// each holding the same value. `a` is expected not to hold `skip`; if it does, that entry is
/// compared like any other.
pub fn same_object_except(a: &Object, b: &Object, skip: &str) -> bool {
	let mut b = b.iter().filter(|(k, _)| k.as_str() != skip);
	for (ka, va) in a.iter() {
		match b.next() {
			Some((kb, vb)) if ka == kb && same_value(va, vb) => {}
			_ => return false,
		}
	}
	b.next().is_none()
}

/// Two numbers of one kind that are the same under that kind's identity.
fn same_number(a: &Number, b: &Number) -> bool {
	match (a, b) {
		(Number::Int(x), Number::Int(y)) => x == y,
		(Number::Float(x), Number::Float(y)) => x.to_bits() == y.to_bits(),
		(Number::Decimal(x), Number::Decimal(y)) => x.serialize() == y.serialize(),
		_ => false,
	}
}

/// [`same_value`] over two objects: the same keys in the same order, each holding the same
/// value.
fn same_object(a: &Object, b: &Object) -> bool {
	a.len() == b.len()
		&& a.iter().zip(b.iter()).all(|((ka, va), (kb, vb))| ka == kb && same_value(va, vb))
}

/// Two bounds of one kind whose endpoints are the same under `same`.
fn same_bound<T>(a: &Bound<T>, b: &Bound<T>, same: impl Fn(&T, &T) -> bool) -> bool {
	match (a, b) {
		(Bound::Included(x), Bound::Included(y)) | (Bound::Excluded(x), Bound::Excluded(y)) => {
			same(x, y)
		}
		(Bound::Unbounded, Bound::Unbounded) => true,
		_ => false,
	}
}

/// [`same_value`] over two geometries: of one kind and shape, every coordinate of the same bits,
/// so `0.0` and `-0.0` differ.
fn same_geometry(a: &super::Geometry, b: &super::Geometry) -> bool {
	use super::Geometry;
	fn coord(a: &geo_types::Coord<f64>, b: &geo_types::Coord<f64>) -> bool {
		a.x.to_bits() == b.x.to_bits() && a.y.to_bits() == b.y.to_bits()
	}
	fn line(a: &geo_types::LineString<f64>, b: &geo_types::LineString<f64>) -> bool {
		a.0.len() == b.0.len() && a.0.iter().zip(&b.0).all(|(a, b)| coord(a, b))
	}
	fn polygon(a: &geo_types::Polygon<f64>, b: &geo_types::Polygon<f64>) -> bool {
		line(a.exterior(), b.exterior())
			&& a.interiors().len() == b.interiors().len()
			&& a.interiors().iter().zip(b.interiors()).all(|(a, b)| line(a, b))
	}
	fn all<T>(a: &[T], b: &[T], same: impl Fn(&T, &T) -> bool) -> bool {
		a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same(a, b))
	}
	match (a, b) {
		(Geometry::Point(x), Geometry::Point(y)) => coord(&x.0, &y.0),
		(Geometry::Line(x), Geometry::Line(y)) => line(x, y),
		(Geometry::Polygon(x), Geometry::Polygon(y)) => polygon(x, y),
		(Geometry::MultiPoint(x), Geometry::MultiPoint(y)) => {
			all(&x.0, &y.0, |a, b| coord(&a.0, &b.0))
		}
		(Geometry::MultiLine(x), Geometry::MultiLine(y)) => all(&x.0, &y.0, line),
		(Geometry::MultiPolygon(x), Geometry::MultiPolygon(y)) => all(&x.0, &y.0, polygon),
		(Geometry::Collection(x), Geometry::Collection(y)) => all(x, y, same_geometry),
		_ => false,
	}
}
