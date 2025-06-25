use crate::expr;
use crate::expr::escape::EscapeRid;
use crate::val::{Array, Number, Object, Range, Strand, Uuid, Value};
use futures::StreamExt;
use nanoid::nanoid;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Bound;
use ulid::Ulid;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Id")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RecordIdKeyRange {
	pub start: Bound<RecordIdKey>,
	pub end: Bound<RecordIdKey>,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Id")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum RecordIdKey {
	Number(i64),
	String(String),
	Uuid(Uuid),
	Object(Object),
	Array(Array),
	Range(Box<RecordIdKeyRange>),
}

impl RecordIdKey {
	/// Generate a new random ID
	pub fn rand() -> Self {
		Self::String(nanoid!(20, &ID_CHARS))
	}
	/// Generate a new random ULID
	pub fn ulid() -> Self {
		Self::String(Ulid::new().to_string())
	}
	/// Generate a new random UUID
	pub fn uuid() -> Self {
		Self::Uuid(Uuid::new_v7())
	}

	/// Returns if this key is a range.
	pub fn is_range(&self) -> bool {
		matches!(self, RecordIdKey::Range(_))
	}

	/// Returns surrealql value of this key.
	pub fn into_value(self) -> Value {
		match self {
			RecordIdKey::Number(n) => Value::Number(Number::Int(n)),
			RecordIdKey::String(s) => Value::Strand(Strand(s)),
			RecordIdKey::Uuid(u) => Value::Uuid(u),
			RecordIdKey::Object(object) => Value::Object(object),
			RecordIdKey::Array(array) => Value::Array(array),
			RecordIdKey::Range(range) => Value::Range(Box::new(Range {
				start: range.start.map(RecordIdKey::into_value),
				end: range.end.map(RecordIdKey::into_value),
			})),
		}
	}

	/// Returns the expression which evaluates to the same value
	pub fn into_literal(self) -> expr::RecordIdKeyLit {
		match self {
			RecordIdKey::Number(n) => expr::RecordIdKeyLit::Number(n),
			RecordIdKey::String(s) => expr::RecordIdKeyLit::String(s),
			RecordIdKey::Uuid(uuid) => expr::RecordIdKeyLit::Uuid(uuid),
			RecordIdKey::Object(object) => expr::RecordIdKeyLit::Object(object.into_literal()),
			RecordIdKey::Array(array) => expr::RecordIdKeyLit::Array(array.into_literal()),
			RecordIdKey::Range(range) => {
				let start = range.start.map(|x| x.into_literal());
				let end = range.end.map(|x| x.into_literal());
				expr::RecordIdKeyLit::Range(Box::new(expr::RecordIdKeyRangeLit {
					start,
					end,
				}))
			}
		}
	}
}

impl PartialEq<Value> for RecordIdKey {
	fn eq(&self, other: &Value) -> bool {
		match self {
			RecordIdKey::Number(a) => Value::Number(Number::Int(a)) == other,
			RecordIdKey::String(a) => {
				if let Value::Strand(b) = other {
					a == b
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
				if let Value::Object(b) = other {
					a == b
				} else {
					false
				}
			}
			RecordIdKey::Range(a) => {
				if let Value::Range(b) = other {
					match a.start {
						Bound::Included(a) => {
							if let Bound::Included(b) = b.start {
								a == b
							} else {
								false
							}
						}
						Bound::Excluded(a) => {
							if let Bound::Excluded(b) = b.start {
								a == b
							} else {
								false
							}
						}
						Bound::Unbounded => matches!(b.start, Bound::Unbounded),
					}
					&&match a.end {
						Bound::Included(a) => {
							if let Bound::Included(b) = b.end {
								a == b
							} else {
								false
							}
						}
						Bound::Excluded(a) => {
							if let Bound::Excluded(b) = b.end {
								a == b
							} else {
								false
							}
						}
						Bound::Unbounded => matches!(b.end, Bound::Unbounded),
					}
				}
			}
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Thing")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RecordId {
	pub table: String,
	pub key: RecordIdKey,
}

impl RecordId {
	/// Creates a new record id from the given table and key
	pub fn new<K>(table: String, key: K) -> Self
	where
		RecordIdKey: From<K>,
	{
		RecordId {
			table,
			key: key.into(),
		}
	}

	pub fn random_for_table(table: String) -> Self {
		RecordId {
			table,
			key: RecordIdKey::rand(),
		}
	}

	/// Turns the record id into a literal which resolves to the same value.
	pub fn into_literal(self) -> expr::RecordIdLit {
		expr::RecordIdLit {
			tb: self.table,
			id: self.key.into_literal(),
		}
	}
}

impl fmt::Display for RecordId {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", EscapeRid(&self.table), self.key)
	}
}
