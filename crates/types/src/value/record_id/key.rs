use serde::{Deserialize, Serialize};

use crate::{Array, Number, Object, Range, RecordIdKeyRange, Uuid, Value};

/// Represents a key component of a record identifier in SurrealDB
///
/// Record identifiers can have various types of keys including numbers, strings, UUIDs,
/// arrays, objects, or ranges. This enum provides type-safe representation for all key types.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum RecordIdKey {
	/// A numeric key
	Number(i64),
	/// A string key
	String(String),
	/// A UUID key
	Uuid(Uuid),
	/// An array key
	Array(Array),
	/// An object key
	Object(Object),
	/// A range key
	Range(Box<RecordIdKeyRange>),
}

impl RecordIdKey {
	/// Returns surrealql value of this key.
	pub fn into_value(self) -> Value {
		match self {
			RecordIdKey::Number(n) => Value::Number(Number::Int(n)),
			RecordIdKey::String(s) => Value::String(s),
			RecordIdKey::Uuid(u) => Value::Uuid(u),
			RecordIdKey::Object(object) => Value::Object(object),
			RecordIdKey::Array(array) => Value::Array(array),
			RecordIdKey::Range(range) => Value::Range(Box::new(Range {
				start: range.start.map(RecordIdKey::into_value),
				end: range.end.map(RecordIdKey::into_value),
			})),
		}
	}

	/// Tries to convert a value into a record id key,
	///
	/// Returns None if the value cannot be converted.
	pub fn from_value(value: Value) -> Option<Self> {
		// NOTE: This method dictates how coversion between values and record id keys
		// behave. This method is reimplementing previous (before expr inversion pr)
		// behavior but I am not sure if it is the right one, float and decimal
		// generaly implicitly convert to other number types but here they are
		// rejected.
		match value {
			Value::Number(Number::Int(i)) => Some(RecordIdKey::Number(i)),
			Value::String(s) => Some(RecordIdKey::String(s)),
			// NOTE: This was previously (before expr inversion pr) also rejected in this
			// conversion, a bug I assume.
			Value::Uuid(uuid) => Some(RecordIdKey::Uuid(uuid)),
			Value::Array(array) => Some(RecordIdKey::Array(array)),
			Value::Object(object) => Some(RecordIdKey::Object(object)),
			Value::Range(range) => {
				RecordIdKeyRange::from_value_range(*range).map(|x| RecordIdKey::Range(Box::new(x)))
			}
			_ => None,
		}
	}
}

impl From<i64> for RecordIdKey {
	fn from(value: i64) -> Self {
		RecordIdKey::Number(value)
	}
}

impl From<String> for RecordIdKey {
	fn from(value: String) -> Self {
		RecordIdKey::String(value)
	}
}

impl From<&str> for RecordIdKey {
	fn from(value: &str) -> Self {
		RecordIdKey::String(value.to_string())
	}
}

impl From<Uuid> for RecordIdKey {
	fn from(value: Uuid) -> Self {
		RecordIdKey::Uuid(value)
	}
}
impl From<Object> for RecordIdKey {
	fn from(value: Object) -> Self {
		RecordIdKey::Object(value)
	}
}
impl From<Array> for RecordIdKey {
	fn from(value: Array) -> Self {
		RecordIdKey::Array(value)
	}
}
impl From<Box<RecordIdKeyRange>> for RecordIdKey {
	fn from(value: Box<RecordIdKeyRange>) -> Self {
		RecordIdKey::Range(value)
	}
}

impl PartialEq<Value> for RecordIdKey {
	fn eq(&self, other: &Value) -> bool {
		match self {
			RecordIdKey::Number(a) => Value::Number(Number::Int(*a)) == *other,
			RecordIdKey::String(a) => {
				if let Value::String(b) = other {
					a.as_str() == b.as_str()
				} else {
					false
				}
			}
			RecordIdKey::Uuid(a) => {
				if let Value::Uuid(b) = other {
					a == b
				} else {
					false
				}
			}
			RecordIdKey::Object(a) => {
				if let Value::Object(b) = other {
					a == b
				} else {
					false
				}
			}
			RecordIdKey::Array(a) => {
				if let Value::Array(b) = other {
					a == b
				} else {
					false
				}
			}
			RecordIdKey::Range(a) => {
				if let Value::Range(b) = other {
					**a == **b
				} else {
					false
				}
			}
		}
	}
}
