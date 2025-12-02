use std::ops::Deref;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};
use surrealdb_types::{SqlFormat, ToSql};

use crate::fmt::EscapeIdent;
use crate::val::IndexFormat;

/// A value type referencing a specific table.
#[revisioned(revision = 1)]
#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	Ord,
	PartialOrd,
	Serialize,
	Deserialize,
	Hash,
	Encode,
	BorrowDecode,
)]
#[serde(rename = "$surrealdb::private::sql::Table")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
pub struct Table(String);

impl Table {
	/// Create a new strand, returns None if the string contains a null byte.
	pub fn new(s: String) -> Table {
		Table(s)
	}

	pub fn into_string(self) -> String {
		self.0
	}

	pub fn as_str(&self) -> &str {
		&self.0
	}

	pub fn is_table_type(&self, tables: &[String]) -> bool {
		tables.is_empty() || tables.contains(&self.0)
	}
}

impl Deref for Table {
	type Target = str;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<surrealdb_types::Table> for Table {
	fn from(value: surrealdb_types::Table) -> Self {
		Table(value.into_string())
	}
}

impl From<Table> for surrealdb_types::Table {
	fn from(value: Table) -> Self {
		surrealdb_types::Table::new(value.0)
	}
}

impl ToSql for Table {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		EscapeIdent(&self.0).fmt_sql(f, sql_fmt);
	}
}
