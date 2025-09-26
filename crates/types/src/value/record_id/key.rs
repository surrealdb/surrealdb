use std::fmt::{self, Debug};

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::{Array, Kind, Number, Object, Range, RecordIdKeyRange, SurrealValue, Uuid, Value};

/// The characters which are supported in server record IDs
pub const ID_CHARS: [char; 36] = [
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

/// Represents a key component of a record identifier in SurrealDB
///
/// Record identifiers can have various types of keys including numbers, strings, UUIDs,
/// arrays, objects, or ranges. This enum provides type-safe representation for all key types.
#[revisioned(revision = 1)]
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
	/// Generate a new random ID
	pub fn rand() -> Self {
		Self::String(nanoid::nanoid!(20, &ID_CHARS))
	}
	/// Generate a new random ULID
	pub fn ulid() -> Self {
		Self::String(ulid::Ulid::new().to_string())
	}
	/// Generate a new random UUID
	pub fn uuid() -> Self {
		Self::Uuid(Uuid::new_v7())
	}

	/// Returns if this key is a range.
	pub fn is_range(&self) -> bool {
		matches!(self, RecordIdKey::Range(_))
	}
}

impl SurrealValue for RecordIdKey {
	fn kind_of() -> Kind {
		Kind::Record(vec![])
	}

	fn is_value(value: &Value) -> bool {
		matches!(value, Value::RecordId(_))
	}

	/// Returns surrealql value of this key.
	fn into_value(self) -> Value {
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
	fn from_value(value: Value) -> anyhow::Result<Self> {
		// NOTE: This method dictates how coversion between values and record id keys
		// behave. This method is reimplementing previous (before expr inversion pr)
		// behavior but I am not sure if it is the right one, float and decimal
		// generaly implicitly convert to other number types but here they are
		// rejected.
		match value {
			Value::Number(Number::Int(i)) => Ok(RecordIdKey::Number(i)),
			Value::String(s) => Ok(RecordIdKey::String(s)),
			// NOTE: This was previously (before expr inversion pr) also rejected in this
			// conversion, a bug I assume.
			Value::Uuid(uuid) => Ok(RecordIdKey::Uuid(uuid)),
			Value::Array(array) => Ok(RecordIdKey::Array(array)),
			Value::Object(object) => Ok(RecordIdKey::Object(object)),
			Value::Range(range) => {
				RecordIdKeyRange::from_value_range(*range).map(|x| RecordIdKey::Range(Box::new(x)))
			}
			_ => Err(anyhow::anyhow!("Failed to convert to RecordIdKey")),
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

impl fmt::Display for RecordIdKey {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			RecordIdKey::Number(n) => write!(f, "{n}"),
			RecordIdKey::String(v) => write!(f, "{v}"),
			RecordIdKey::Uuid(uuid) => std::fmt::Display::fmt(uuid, f),
			RecordIdKey::Object(object) => std::fmt::Display::fmt(object, f),
			RecordIdKey::Array(array) => std::fmt::Display::fmt(array, f),
			RecordIdKey::Range(rid) => rid.fmt(f),
		}
	}
}
