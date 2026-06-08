use std::borrow::{Borrow, Cow};
use std::fmt::{self, Display};
use std::io::{BufRead, Read, Write};
use std::ops::Deref;

use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned, SkipRevisioned};
use storekey::{BorrowDecode, Decode, Encode};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::fmt::EscapeIdent;

/// A value type referencing a specific table.
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

impl Deref for TableName {
	type Target = str;
	fn deref(&self) -> &Self::Target {
		self.0.as_str()
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

impl<'a> From<TableName> for Cow<'a, str> {
	fn from(value: TableName) -> Self {
		Cow::Owned(value.into_string())
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

impl Revisioned for TableName {
	fn revision() -> u16 {
		String::revision()
	}
}

impl SerializeRevisioned for TableName {
	fn serialize_revisioned<W: Write>(&self, w: &mut W) -> Result<(), revision::Error> {
		<str as SerializeRevisioned>::serialize_revisioned(self.as_str(), w)
	}
}

impl DeserializeRevisioned for TableName {
	fn deserialize_revisioned<R: Read>(r: &mut R) -> Result<Self, revision::Error> {
		// Route through `Strand` so we get its zero-/single-alloc
		// decode paths directly, rather than materialising an
		// intermediate `String` that would force an extra allocation
		// on every long table name loaded from disk or the wire.
		Ok(TableName(Strand::deserialize_revisioned(r)?))
	}
}

impl SkipRevisioned for TableName {
	fn skip_revisioned<R: Read>(r: &mut R) -> Result<(), revision::Error> {
		<Strand as SkipRevisioned>::skip_revisioned(r)
	}
}

impl revision::WalkRevisioned for TableName {
	type Walker<'r, R: revision::BorrowedReader + 'r> = revision::LeafWalker<'r, TableName, R>;

	fn walk_revisioned<'r, R: revision::BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, revision::Error> {
		Ok(revision::LeafWalker::new(reader))
	}
}

impl revision::LengthPrefixedBytes for TableName {}

impl<F> Encode<F> for TableName {
	fn encode<W: Write>(&self, w: &mut storekey::Writer<W>) -> Result<(), storekey::EncodeError> {
		<str as Encode<F>>::encode::<W>(self.as_str(), w)
	}
}

impl<'de, F> BorrowDecode<'de, F> for TableName {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> Result<Self, storekey::DecodeError> {
		Ok(TableName(<Strand as BorrowDecode<'de, F>>::borrow_decode(r)?))
	}
}

impl<F> Decode<F> for TableName {
	fn decode<R: BufRead>(r: &mut storekey::Reader<R>) -> Result<Self, storekey::DecodeError> {
		Ok(TableName(<Strand as Decode<F>>::decode(r)?))
	}
}

#[cfg(test)]
mod tests {
	use revision::{SerializeRevisioned, WalkRevisioned};

	use super::TableName;

	#[test]
	fn table_name_with_bytes_matches_serialize() {
		let name = TableName::from("users");
		let mut bytes = Vec::new();
		name.serialize_revisioned(&mut bytes).unwrap();
		let mut r = bytes.as_slice();
		let walker = TableName::walk_revisioned(&mut r).unwrap();
		let observed = walker.with_bytes(|raw| raw.to_vec()).unwrap();
		assert_eq!(observed.as_slice(), b"users");
		assert!(r.is_empty());
	}
}
