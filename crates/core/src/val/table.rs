use std::borrow::{Borrow, Cow};
use std::fmt::{self, Display};
use std::ops::Deref;

use revision::revisioned;
use storekey::{BorrowDecode, Encode};
use surrealdb_types::{SqlFormat, ToSql};

use crate::fmt::EscapeIdent;
use crate::val::IndexFormat;

/// A value type referencing a specific table.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Encode, BorrowDecode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
#[repr(transparent)]
pub struct TableName(String);

impl TableName {
	/// Create a new strand, returns None if the string contains a null byte.
	pub fn new(s: String) -> TableName {
		TableName(s)
	}

	pub fn into_string(self) -> String {
		self.0
	}

	pub fn as_str(&self) -> &str {
		&self.0
	}

	pub fn is_table_type(&self, tables: &[TableName]) -> bool {
		tables.is_empty() || tables.contains(self)
	}
}

impl Deref for TableName {
	type Target = str;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<String> for TableName {
	fn from(value: String) -> Self {
		TableName(value)
	}
}

impl From<TableName> for String {
	fn from(value: TableName) -> Self {
		value.0
	}
}

impl From<&str> for TableName {
	fn from(value: &str) -> Self {
		TableName(value.to_string())
	}
}

impl From<surrealdb_types::Table> for TableName {
	fn from(value: surrealdb_types::Table) -> Self {
		TableName(value.into_string())
	}
}

impl From<TableName> for surrealdb_types::Table {
	fn from(value: TableName) -> Self {
		surrealdb_types::Table::new(value.0)
	}
}

impl<'a> From<TableName> for Cow<'a, str> {
	fn from(value: TableName) -> Self {
		Cow::Owned(value.0)
	}
}

impl ToSql for TableName {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		EscapeIdent(&self.0).fmt_sql(f, sql_fmt);
	}
}

impl PartialEq<TableName> for &TableName {
	fn eq(&self, other: &TableName) -> bool {
		self.0 == other.0
	}
}

impl PartialEq<str> for TableName {
	fn eq(&self, other: &str) -> bool {
		self.0 == other
	}
}

impl PartialEq<TableName> for str {
	fn eq(&self, other: &TableName) -> bool {
		self == other.0
	}
}

impl PartialEq<&str> for TableName {
	fn eq(&self, other: &&str) -> bool {
		self.0 == *other
	}
}

impl PartialEq<String> for TableName {
	fn eq(&self, other: &String) -> bool {
		self.0 == *other
	}
}

impl AsRef<str> for TableName {
	fn as_ref(&self) -> &str {
		&self.0
	}
}

impl Display for TableName {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl Borrow<str> for TableName {
	fn borrow(&self) -> &str {
		&self.0
	}
}
