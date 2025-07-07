use crate::cnf::ID_CHARS;
use crate::expr;
use crate::expr::escape::EscapeRid;
use crate::val::{Array, Number, Object, Range, Strand, Uuid, Value};
use futures::StreamExt;
use nanoid::nanoid;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::ops::Bound;
use ulid::Ulid;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Id")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RecordIdKeyRange {
	pub start: Bound<RecordIdKey>,
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

impl fmt::Display for RecordIdKeyRange {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self.start {
			Bound::Unbounded => {}
			Bound::Included(ref x) => write!(f, "{x}")?,
			Bound::Excluded(ref x) => write!(f, "{x}>")?,
		}
		write!(f, "..")?;
		match self.end {
			Bound::Unbounded => {}
			Bound::Included(ref x) => write!(f, "={x}")?,
			Bound::Excluded(ref x) => write!(f, "{x}")?,
		}
		Ok(())
	}
}

impl RecordIdKeyRange {
	/// Convertes a record id key range into the range from a normal value.
	pub fn into_value_range(self) -> Range {
		Range {
			start: self.start.map(|x| x.into_value()),
			end: self.end.map(|x| x.into_value()),
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Id")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum RecordIdKey {
	Number(i64),
	//TODO: This should definitely be strand, not string as null bytes here can cause a lot of
	//issues.
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

impl From<i64> for RecordIdKey {
	fn from(value: i64) -> Self {
		RecordIdKey::Number(value)
	}
}

impl From<Strand> for RecordIdKey {
	fn from(value: String) -> Self {
		RecordIdKey::String(value)
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
				if let Value::Strand(b) = other {
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
			RecordIdKey::String(v) => EscapeRid(v).fmt(f),
			RecordIdKey::Uuid(uuid) => uuid.fmt(f),
			RecordIdKey::Object(object) => object.fmt(f),
			RecordIdKey::Array(array) => array.fmt(f),
			RecordIdKey::Range(rid) => rid.fmt(f),
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

	/// Returns the string representation of this record id without
	pub fn into_raw_string(&self) -> String {
		todo!()
	}
}

impl fmt::Display for RecordId {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", EscapeRid(&self.table), self.key)
	}
}
