use crate::expr::escape::EscapeRid;
use crate::expr::{self, Uuid};
use crate::val::{Array, Object};
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Thing")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RecordId {
	pub table: String,
	pub key: RecordIdKey,
}

impl RecordId {
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
