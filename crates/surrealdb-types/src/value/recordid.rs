use std::cmp::Ordering;
use std::fmt::Display;
use std::ops::Bound;

use serde::{Deserialize, Serialize};

use crate::utils::escape::{Escape, QuoteStr};
use crate::{Array, Number, Object, Range, Uuid, Value};

/// Represents a range of record identifier keys in SurrealDB
///
/// This type is used for range queries on record identifiers,
/// allowing queries like "find all records with IDs between X and Y".
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RecordIdKeyRange {
	/// The lower bound of the range
	pub start: Bound<RecordIdKey>,
	/// The upper bound of the range
	pub end: Bound<RecordIdKey>,
}

impl PartialOrd for RecordIdKeyRange {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for RecordIdKeyRange {
	fn cmp(&self, other: &Self) -> Ordering {
		fn compare_bounds(a: &Bound<RecordIdKey>, b: &Bound<RecordIdKey>) -> Ordering {
			match a {
				Bound::Unbounded => match b {
					Bound::Unbounded => Ordering::Equal,
					_ => Ordering::Less,
				},
				Bound::Included(a) => match b {
					Bound::Unbounded => Ordering::Greater,
					Bound::Included(b) => a.cmp(b),
					Bound::Excluded(_) => Ordering::Less,
				},
				Bound::Excluded(a) => match b {
					Bound::Excluded(b) => a.cmp(b),
					_ => Ordering::Greater,
				},
			}
		}
		match compare_bounds(&self.start, &other.end) {
			Ordering::Equal => compare_bounds(&self.end, &other.end),
			x => x,
		}
	}
}

impl PartialEq<Range> for RecordIdKeyRange {
	fn eq(&self, other: &Range) -> bool {
		(match self.start {
			Bound::Included(ref a) => {
				if let Bound::Included(ref b) = other.start {
					a == b
				} else {
					false
				}
			}
			Bound::Excluded(ref a) => {
				if let Bound::Excluded(ref b) = other.start {
					a == b
				} else {
					false
				}
			}
			Bound::Unbounded => matches!(other.start, Bound::Unbounded),
		}) && (match self.end {
			Bound::Included(ref a) => {
				if let Bound::Included(ref b) = other.end {
					a == b
				} else {
					false
				}
			}
			Bound::Excluded(ref a) => {
				if let Bound::Excluded(ref b) = other.end {
					a == b
				} else {
					false
				}
			}
			Bound::Unbounded => matches!(other.end, Bound::Unbounded),
		})
	}
}

impl Display for RecordIdKeyRange {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match &self.start {
			Bound::Unbounded => (),
			Bound::Included(start) => write!(f, "{}", start)?,
			Bound::Excluded(start) => write!(f, "{}>", start)?,
		}

		write!(f, "..")?;

		match &self.end {
			Bound::Unbounded => (),
			Bound::Included(end) => write!(f, "={}", end)?,
			Bound::Excluded(end) => write!(f, "{}", end)?,
		}

		Ok(())
	}
}

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

impl Display for RecordIdKey {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			RecordIdKey::Number(x) => write!(f, "{}", x),
			RecordIdKey::String(x) => write!(f, "{}", escape_ident(x)),
			RecordIdKey::Uuid(x) => write!(f, "u{}", QuoteStr(&x.0.to_string())),
			RecordIdKey::Array(x) => write!(f, "{}", x),
			RecordIdKey::Object(x) => write!(f, "{}", x),
			RecordIdKey::Range(x) => write!(f, "{}", x),
		}
	}
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
/// Represents a record identifier in SurrealDB
///
/// A record identifier consists of a table name and a key that uniquely identifies
/// a record within that table. This is the primary way to reference specific records.
pub struct RecordId {
	/// The name of the table containing the record
	pub table: String,
	/// The key that uniquely identifies the record within the table
	pub key: RecordIdKey,
}

impl Display for RecordId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let inner = format!("{}:{}", escape_ident(&self.table), self.key);
		write!(f, "r{}", QuoteStr(&inner))
	}
}

fn escape_ident(v: &str) -> String {
	if v.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_')) {
		v.to_string()
	} else {
		format!("`{}`", Escape::escape_str(v, '`'))
	}
}
