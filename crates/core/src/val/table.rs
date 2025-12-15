use std::ops::Deref;

use revision::revisioned;
use storekey::{BorrowDecode, Encode};
use surrealdb_types::{SqlFormat, ToSql};

use crate::fmt::EscapeIdent;
use crate::val::{IndexFormat, Symbol};

/// A value type referencing a specific table.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, BorrowDecode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
pub struct Table(Symbol);

impl Table {
	/// Create a new table from a string
	pub fn new(s: impl Into<Symbol>) -> Table {
		Table(s.into())
	}

	/// Convert the table to a string
	pub fn into_string(self) -> String {
		self.0.into()
	}

	/// Get the underlying table as a slice
	pub fn as_str(&self) -> &str {
		&self.0
	}

	/// Check if the table is one of the given tables
	pub fn is_table_type(&self, tables: &[String]) -> bool {
		tables.is_empty() || tables.iter().any(|t| t == self.as_str())
	}
}

impl Deref for Table {
	type Target = str;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<Symbol> for Table {
	fn from(value: Symbol) -> Self {
		Self(value)
	}
}

impl From<String> for Table {
	fn from(value: String) -> Self {
		Self(value.into())
	}
}

impl From<&String> for Table {
	fn from(value: &String) -> Self {
		Self(value.into())
	}
}

impl From<surrealdb_types::Table> for Table {
	fn from(value: surrealdb_types::Table) -> Self {
		Self::from(value.into_string())
	}
}

impl From<Table> for String {
	fn from(value: Table) -> Self {
		value.0.into()
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
