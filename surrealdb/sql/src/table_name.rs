use std::borrow::{Borrow, Cow};
use std::fmt::{self, Display};

use common::fmt::EscapeIdent;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

/// A table name as it appears in the syntax tree.
///
/// This is the form that renders: [`ToSql`] escapes the name when it would
/// otherwise be read back as a reserved word, which is what makes
/// `DEFINE TABLE ⟨select⟩` round-trip. It is also the form that crosses the
/// public boundary, since the wire's `surrealdb_types::Table` is a language
/// value rather than an engine or storage one.
///
/// Every AST node that renders a table name goes through that impl rather than
/// naming an escaper itself, so how a table name escapes is decided in one
/// place. A node that reaches for `EscapeIdent` directly is not wrong today,
/// but it is a second place to change.
///
/// Lowering converts this to `surrealdb_strand::TableName`, which the engine
/// passes around and which carries neither capability.
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[repr(transparent)]
pub struct TableName(Strand);

impl TableName {
	/// Create a new table name.
	pub fn new(s: impl Into<Strand>) -> TableName {
		TableName(s.into())
	}

	pub fn into_string(self) -> String {
		self.0.into()
	}

	pub fn as_str(&self) -> &str {
		self.0.as_str()
	}

	pub fn is_table_type(&self, tables: &[TableName]) -> bool {
		tables.is_empty() || tables.contains(self)
	}
}

impl From<String> for TableName {
	fn from(value: String) -> Self {
		TableName(value.into())
	}
}

impl From<TableName> for String {
	fn from(value: TableName) -> Self {
		value.0.into()
	}
}

impl From<&str> for TableName {
	fn from(value: &str) -> Self {
		TableName(Strand::from(value))
	}
}

impl From<Strand> for TableName {
	fn from(value: Strand) -> Self {
		TableName(value)
	}
}

impl From<TableName> for Strand {
	fn from(value: TableName) -> Self {
		value.0
	}
}

impl<'a> From<TableName> for Cow<'a, str> {
	fn from(value: TableName) -> Self {
		Cow::Owned(value.into_string())
	}
}

/// Lowering: the syntax tree's name becomes the engine's.
impl From<TableName> for surrealdb_strand::TableName {
	fn from(value: TableName) -> Self {
		surrealdb_strand::TableName::from(value.0)
	}
}

/// Rendering: the engine's name becomes the syntax tree's, so that it prints
/// with the escaping this layer owns.
impl From<surrealdb_strand::TableName> for TableName {
	fn from(value: surrealdb_strand::TableName) -> Self {
		TableName(Strand::from(value))
	}
}

impl From<surrealdb_types::Table> for TableName {
	fn from(value: surrealdb_types::Table) -> Self {
		TableName(Strand::from(value.into_string()))
	}
}

impl From<TableName> for surrealdb_types::Table {
	fn from(value: TableName) -> Self {
		surrealdb_types::Table::new(value.into_string())
	}
}

impl ToSql for TableName {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		EscapeIdent(self.as_str()).fmt_sql(f, sql_fmt);
	}
}

impl PartialEq<TableName> for &TableName {
	fn eq(&self, other: &TableName) -> bool {
		self.0 == other.0
	}
}

impl PartialEq<str> for TableName {
	fn eq(&self, other: &str) -> bool {
		self.as_str() == other
	}
}

impl PartialEq<TableName> for str {
	fn eq(&self, other: &TableName) -> bool {
		self == other.as_str()
	}
}

impl PartialEq<&str> for TableName {
	fn eq(&self, other: &&str) -> bool {
		self.as_str() == *other
	}
}

impl PartialEq<String> for TableName {
	fn eq(&self, other: &String) -> bool {
		self.as_str() == other.as_str()
	}
}

impl AsRef<str> for TableName {
	fn as_ref(&self) -> &str {
		self.as_str()
	}
}

impl Display for TableName {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		Display::fmt(self.as_str(), f)
	}
}

impl Borrow<str> for TableName {
	fn borrow(&self) -> &str {
		self.as_str()
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_types::ToSql;

	use super::TableName;

	/// Escaping is what distinguishes this form from the engine's, so a name
	/// that collides with a keyword must come back quoted.
	#[test]
	fn renders_with_keyword_escaping() {
		assert_eq!(TableName::from("users").to_sql(), "users");
		assert_eq!(TableName::from("select").to_sql(), "`select`");
		assert_eq!(TableName::from("a b").to_sql(), "`a b`");
	}

	#[test]
	fn lowering_round_trips_through_the_engine_form() {
		let language = TableName::from("users");
		let engine: surrealdb_strand::TableName = language.clone().into();
		assert_eq!(TableName::from(engine), language);
	}
}
