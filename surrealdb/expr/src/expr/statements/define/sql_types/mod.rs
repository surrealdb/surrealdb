//! Validation of a written value against the declared SQL type of its field.
//!
//! [`validate`] is the one entry the write path calls, after the value has been coerced to the
//! field's kind. Each type family has its own module with a `validate` of the same shape that
//! answers `None` for a declared type it does not own, so the families compose without knowing
//! of each other. A family returns the value to store, which may be canonicalised (rounded,
//! trimmed, padded), or the violation to report.

pub mod number_shaped;
pub mod range;

use super::declared_type::{DeclaredSqlType, TypeViolation};
use crate::val::Value;

mod core;
pub mod geometry;
mod json;
pub mod text_form;
pub mod text_shaped;

/// The value to store for a field declared as `declared`, or the violation that refuses it.
/// `column` names the field in a violation message. A declared type that no family owns accepts
/// the value unchanged.
pub fn validate(
	declared: &DeclaredSqlType,
	value: Value,
	column: &str,
) -> Result<Value, TypeViolation> {
	// An array type holds its declared element type in each element, at every dimension. A
	// multidimensional array is rectangular, as PostgreSQL reads and writes one: every sub-array
	// at a depth has the same length and the same nesting.
	if let Some(element) = declared.element() {
		let Value::Array(items) = value else {
			return Ok(value);
		};
		if declared.array_dimensions().len() > 1
			&& !items.0.is_empty()
			&& rectangular_dims(&items.0).is_none()
		{
			return Err(TypeViolation {
				code: "2202E",
				message: "multidimensional arrays must have array expressions with matching \
				          dimensions"
					.to_string(),
			});
		}
		return items
			.0
			.into_iter()
			.map(|item| validate(&element, item, column))
			.collect::<Result<Vec<Value>, TypeViolation>>()
			.map(|items| Value::Array(crate::val::Array(items)));
	}
	// Each family is tried in turn; the first that owns the declared type decides. A family that
	// declines hands the value back unchanged.
	let families: &[Family] = &[
		core::validate,
		json::validate,
		number_shaped::validate,
		geometry::validate,
		text_shaped::validate,
		range::validate,
	];
	let mut value = value;
	for family in families {
		match family(declared, value, column) {
			Validated::Decided(result) => return result,
			Validated::Declined(back) => value = back,
		}
	}
	Ok(value)
}

/// The length of each dimension of a rectangular array, outermost first: `None` for an array
/// with a ragged dimension, an empty sub-array, or arrays and other values side by side.
fn rectangular_dims(items: &[Value]) -> Option<Vec<usize>> {
	if items.is_empty() {
		return None;
	}
	if !items.iter().any(|value| matches!(value, Value::Array(_))) {
		return Some(vec![items.len()]);
	}
	let mut inner: Option<Vec<usize>> = None;
	for value in items {
		let Value::Array(sub) = value else {
			return None;
		};
		let dims = rectangular_dims(&sub.0)?;
		if inner.as_ref().is_some_and(|inner| *inner != dims) {
			return None;
		}
		inner = Some(dims);
	}
	let mut dims = vec![items.len()];
	dims.extend(inner?);
	Some(dims)
}

/// What a family answers for one value.
#[derive(Debug)]
pub enum Validated {
	/// The family owns the declared type: the value to store, or the violation.
	Decided(Result<Value, TypeViolation>),
	/// The family does not own the declared type; the value is handed back unchanged.
	Declined(Value),
}

/// The shape of a family's `validate`.
pub type Family = fn(&DeclaredSqlType, Value, &str) -> Validated;
