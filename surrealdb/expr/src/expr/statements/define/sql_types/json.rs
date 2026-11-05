//! The JSON family: a `json` or `jsonb` column, and MySQL `JSON`, holds only the values JSON can
//! carry.
//!
//! JSON has none, bool, number, string, array, and object. A value of any other kind is refused
//! with SQLSTATE `22032`, and so is a non-finite float, which JSON has no spelling for. The check
//! applies to each element of an array and each member of an object, recursively.

#[cfg(test)]
mod tests;

use super::super::declared_type::{DeclaredSqlType, TypeViolation};
use super::Validated;
use crate::val::{Number, Value};

/// SQLSTATE `22032`, invalid JSON text.
const INVALID_JSON: &str = "22032";

/// The validation of the JSON family. It owns the declared JSON types and declines every other.
pub fn validate(declared: &DeclaredSqlType, value: Value, _column: &str) -> Validated {
	if declared.json_flavour().is_none() {
		return Validated::Declined(value);
	}
	match json_refusal(&value) {
		None => Validated::Decided(Ok(value)),
		Some(kind) => Validated::Decided(Err(TypeViolation {
			code: INVALID_JSON,
			message: format!("JSON cannot hold a {kind}"),
		})),
	}
}

/// The kind of the first part of `value` that JSON cannot carry, or `None` when all of it can be
/// carried.
fn json_refusal(value: &Value) -> Option<&'static str> {
	match value {
		Value::None | Value::Null | Value::Bool(_) | Value::String(_) => None,
		Value::Number(number) if !finite(number) => Some("non-finite number"),
		Value::Number(_) => None,
		Value::Array(items) => items.iter().find_map(json_refusal),
		Value::Object(object) => object.values().find_map(json_refusal),
		refused @ (Value::Duration(_)
		| Value::Datetime(_)
		| Value::Uuid(_)
		| Value::Set(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)) => Some(refused.kind_of()),
	}
}

/// Whether a number has a JSON spelling: every kind but a NaN or infinite float of any width.
fn finite(number: &Number) -> bool {
	match number {
		Number::Float(float) => float.is_finite(),
		Number::F32(float) => float.is_finite(),
		Number::F16(float) => float.is_finite(),
		Number::Int(_)
		| Number::I8(_)
		| Number::U8(_)
		| Number::I16(_)
		| Number::U16(_)
		| Number::I32(_)
		| Number::U32(_)
		| Number::U64(_)
		| Number::Decimal(_)
		| Number::BigDecimal(_) => true,
	}
}
