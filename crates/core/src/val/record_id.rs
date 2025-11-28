use std::cmp::Ordering;
use std::ops::Bound;

use nanoid::nanoid;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};
use surrealdb_types::{SqlFormat, ToSql, write_sql};
use ulid::Ulid;

use crate::cnf::ID_CHARS;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{self, Expr, Field, Fields, Literal, SelectStatement};
use crate::fmt::EscapeRid;
use crate::kvs::impl_kv_value_revisioned;
use crate::val::{Array, IndexFormat, Number, Object, Range, Uuid, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, Encode, BorrowDecode)]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
pub(crate) struct RecordIdKeyRange {
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

impl ToSql for RecordIdKeyRange {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self.start {
			Bound::Unbounded => {}
			Bound::Included(ref x) => write_sql!(f, sql_fmt, "{x}"),
			Bound::Excluded(ref x) => write_sql!(f, sql_fmt, "{x}>"),
		}
		write_sql!(f, sql_fmt, "..");
		match self.end {
			Bound::Unbounded => {}
			Bound::Included(ref x) => write_sql!(f, sql_fmt, "={x}"),
			Bound::Excluded(ref x) => write_sql!(f, sql_fmt, "{x}"),
		}
	}
}

impl TryFrom<RecordIdKeyRange> for crate::types::PublicRecordIdKeyRange {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKeyRange) -> Result<Self, Self::Error> {
		Ok(crate::types::PublicRecordIdKeyRange {
			start: match value.start {
				Bound::Included(x) => Bound::Included(x.try_into()?),
				Bound::Excluded(x) => Bound::Excluded(x.try_into()?),
				Bound::Unbounded => Bound::Unbounded,
			},
			end: match value.end {
				Bound::Included(x) => Bound::Included(x.try_into()?),
				Bound::Excluded(x) => Bound::Excluded(x.try_into()?),
				Bound::Unbounded => Bound::Unbounded,
			},
		})
	}
}

impl From<crate::types::PublicRecordIdKeyRange> for RecordIdKeyRange {
	fn from(value: crate::types::PublicRecordIdKeyRange) -> Self {
		RecordIdKeyRange {
			start: value.start.map(|x| x.into()),
			end: value.end.map(|x| x.into()),
		}
	}
}

impl RecordIdKeyRange {
	pub(crate) fn into_literal(self) -> expr::RecordIdKeyRangeLit {
		let start = self.start.map(|x| x.into_literal());
		let end = self.end.map(|x| x.into_literal());
		expr::RecordIdKeyRangeLit {
			start,
			end,
		}
	}

	/// Convertes a record id key range into the range from a normal value.
	pub(crate) fn into_value_range(self) -> Range {
		Range {
			start: self.start.map(|x| x.into_value()),
			end: self.end.map(|x| x.into_value()),
		}
	}

	/// Convertes a record id key range into the range from a normal value.
	pub(crate) fn from_value_range(range: Range) -> Option<Self> {
		let start = match range.start {
			Bound::Included(x) => Bound::Included(RecordIdKey::from_value(x)?),
			Bound::Excluded(x) => Bound::Excluded(RecordIdKey::from_value(x)?),
			Bound::Unbounded => Bound::Unbounded,
		};
		let end = match range.end {
			Bound::Included(x) => Bound::Included(RecordIdKey::from_value(x)?),
			Bound::Excluded(x) => Bound::Excluded(RecordIdKey::from_value(x)?),
			Bound::Unbounded => Bound::Unbounded,
		};

		Some(RecordIdKeyRange {
			start,
			end,
		})
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
#[derive(
	Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, Encode, BorrowDecode,
)]
#[serde(rename = "$surrealdb::private::sql::Id")]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
pub(crate) enum RecordIdKey {
	Number(i64),
	String(String),
	Uuid(Uuid),
	Array(Array),
	Object(Object),
	Range(Box<RecordIdKeyRange>),
}

impl_kv_value_revisioned!(RecordIdKey);

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
	pub(crate) fn into_value(self) -> Value {
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
	pub(crate) fn from_value(value: Value) -> Option<Self> {
		// NOTE: This method dictates how coversion between values and record id keys
		// behave. This method is reimplementing previous (before expr inversion pr)
		// behavior but I am not sure if it is the right one, float and decimal
		// generaly implicitly convert to other number types but here they are
		// rejected.
		match value {
			Value::Number(Number::Int(i)) => Some(RecordIdKey::Number(i)),
			Value::String(strand) => Some(RecordIdKey::String(strand)),
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

	/// Returns the expression which evaluates to the same value
	pub fn into_literal(self) -> expr::RecordIdKeyLit {
		match self {
			RecordIdKey::Number(n) => expr::RecordIdKeyLit::Number(n),
			RecordIdKey::String(s) => expr::RecordIdKeyLit::String(s),
			RecordIdKey::Uuid(uuid) => expr::RecordIdKeyLit::Uuid(uuid),
			RecordIdKey::Object(object) => expr::RecordIdKeyLit::Object(object.into_literal()),
			RecordIdKey::Array(array) => expr::RecordIdKeyLit::Array(array.into_literal()),
			RecordIdKey::Range(range) => {
				expr::RecordIdKeyLit::Range(Box::new(range.into_literal()))
			}
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

impl From<crate::types::PublicRecordIdKey> for RecordIdKey {
	fn from(value: crate::types::PublicRecordIdKey) -> Self {
		match value {
			crate::types::PublicRecordIdKey::Number(x) => Self::Number(x),
			crate::types::PublicRecordIdKey::String(x) => Self::String(x),
			crate::types::PublicRecordIdKey::Uuid(x) => Self::Uuid(x.into()),
			crate::types::PublicRecordIdKey::Array(x) => Self::Array(x.into()),
			crate::types::PublicRecordIdKey::Object(x) => Self::Object(x.into()),
			crate::types::PublicRecordIdKey::Range(x) => Self::Range(Box::new((*x).into())),
		}
	}
}

impl TryFrom<RecordIdKey> for crate::types::PublicRecordIdKey {
	type Error = anyhow::Error;

	fn try_from(value: RecordIdKey) -> Result<Self, Self::Error> {
		Ok(match value {
			RecordIdKey::Number(x) => Self::Number(x),
			RecordIdKey::String(x) => Self::String(x),
			RecordIdKey::Uuid(x) => Self::Uuid(x.into()),
			RecordIdKey::Array(x) => Self::Array(x.try_into()?),
			RecordIdKey::Object(x) => Self::Object(x.try_into()?),
			RecordIdKey::Range(x) => Self::Range(Box::new((*x).try_into()?)),
		})
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
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			RecordIdKey::Number(n) => write_sql!(f, sql_fmt, "{n}"),
			RecordIdKey::String(v) => write_sql!(f, sql_fmt, "{}", EscapeRid(v)),
			RecordIdKey::Uuid(uuid) => write_sql!(f, sql_fmt, "{}", uuid),
			RecordIdKey::Object(object) => write_sql!(f, sql_fmt, "{}", object),
			RecordIdKey::Array(array) => write_sql!(f, sql_fmt, "{}", array),
			RecordIdKey::Range(rid) => write_sql!(f, sql_fmt, "{}", rid),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(
	Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, Encode, BorrowDecode,
)]
#[serde(rename = "$surrealdb::private::RecordId")]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
pub(crate) struct RecordId {
	pub table: String,
	pub key: RecordIdKey,
}

impl_kv_value_revisioned!(RecordId);

impl RecordId {
	/// Creates a new record id from the given table and key
	pub(crate) fn new<K>(table: String, key: K) -> Self
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
	pub(crate) fn into_literal(self) -> expr::RecordIdLit {
		expr::RecordIdLit {
			table: self.table,
			key: self.key.into_literal(),
		}
	}

	pub fn is_table_type(&self, tables: &[String]) -> bool {
		tables.is_empty() || tables.contains(&self.table)
	}

	pub(crate) async fn select_document(
		self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> anyhow::Result<Option<Object>> {
		// Fetch the record id's contents
		let stm = SelectStatement {
			expr: Fields::Select(vec![Field::All]),
			what: vec![Expr::Literal(Literal::RecordId(self.into_literal()))],
			..SelectStatement::default()
		};
		if let Value::Object(x) = stk.run(|stk| stm.compute(stk, ctx, opt, doc)).await?.first() {
			Ok(Some(x))
		} else {
			Ok(None)
		}
	}
}

impl TryFrom<RecordId> for crate::types::PublicRecordId {
	type Error = anyhow::Error;

	fn try_from(value: RecordId) -> Result<Self, Self::Error> {
		Ok(crate::types::PublicRecordId {
			table: value.table.into(),
			key: value.key.try_into()?,
		})
	}
}

impl From<crate::types::PublicRecordId> for RecordId {
	fn from(value: crate::types::PublicRecordId) -> Self {
		RecordId {
			table: value.table.into_string(),
			key: RecordIdKey::from(value.key),
		}
	}
}

impl ToSql for RecordId {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "{}:{}", EscapeRid(&self.table), self.key)
	}
}
