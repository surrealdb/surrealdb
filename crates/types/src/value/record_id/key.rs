use serde::{Deserialize, Serialize};

use crate::sql::{SqlFormat, ToSql};
// Needed because we use the SurrealValue derive macro inside the crate which exports it :)
use crate::{self as surrealdb_types};
use crate::{Array, Number, Object, RecordIdKeyRange, SurrealValue, Uuid, Value};

/// The characters which are supported in server record IDs
pub const ID_CHARS: [char; 36] = [
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

/// Represents a key component of a record identifier in SurrealDB
///
/// Record identifiers can have various types of keys including numbers, strings, UUIDs,
/// arrays, objects, or ranges. This enum provides type-safe representation for all key types.
#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize, SurrealValue,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[surreal(untagged, lowercase)]
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

impl From<RecordIdKeyRange> for RecordIdKey {
	fn from(value: RecordIdKeyRange) -> Self {
		RecordIdKey::Range(Box::new(value))
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

impl ToSql for RecordIdKey {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		use crate::utils::escape::EscapeRid;

		match self {
			RecordIdKey::Number(n) => n.fmt_sql(f, fmt),
			RecordIdKey::String(v) => EscapeRid(v).fmt_sql(f, fmt),
			RecordIdKey::Uuid(uuid) => uuid.fmt_sql(f, fmt),
			RecordIdKey::Object(object) => object.fmt_sql(f, fmt),
			RecordIdKey::Array(array) => array.fmt_sql(f, fmt),
			RecordIdKey::Range(rid) => rid.fmt_sql(f, fmt),
		}
	}
}
