/// Record id key types
pub mod key;
/// Record id range types
pub mod range;

pub use key::*;
pub use range::*;
use serde::{Deserialize, Serialize};
use surrealdb_types_derive::write_sql;

use crate as surrealdb_types;
use crate::Table;
use crate::sql::{SqlFormat, ToSql};

/// Represents a record identifier in SurrealDB
///
/// A record identifier consists of a table name and a key that uniquely identifies
/// a record within that table. This is the primary way to reference specific records.

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RecordId {
	/// The name of the table containing the record
	pub table: Table,
	/// The key that uniquely identifies the record within the table
	pub key: RecordIdKey,
}

impl RecordId {
	/// Creates a new record id from the given table and key
	pub fn new(table: impl Into<Table>, key: impl Into<RecordIdKey>) -> Self {
		RecordId {
			table: table.into(),
			key: key.into(),
		}
	}

	/// Checks if the record id is of the specified type.
	pub fn is_table_type(&self, tables: &[Table]) -> bool {
		tables.is_empty() || tables.contains(&self.table)
	}

	/// Parses a record id which must be in the format of `table:key`.
	///
	/// Table and key fragments wrapped in backticks (as produced by [`ToSql`]) are unescaped;
	/// unwrapped fragments are kept as-is. The key is always parsed as a
	/// [`RecordIdKey::String`], so `parse_simple(&id.to_sql())` only reproduces the original
	/// id when its key is a string: numeric, uuid, array, object and range keys come back as
	/// the string that was printed for them.
	pub fn parse_simple(s: &str) -> anyhow::Result<Self> {
		use crate::utils::escape::decode_backtick_ident;

		let (table, key) =
			split_table_key(s).ok_or_else(|| anyhow::anyhow!("Invalid record id: {s}"))?;
		Ok(Self::new(decode_backtick_ident(table)?, decode_backtick_ident(key)?))
	}
}

/// Split a `table:key` string at the colon separating the two fragments.
///
/// A backtick-quoted table is scanned to its closing quote first, so a colon inside the
/// quotes (`` `we:ird`:tobie ``) does not split the table name. Backslash escapes are
/// skipped while scanning, so an escaped quote (`` \` ``) does not end the table name.
///
/// Returns `None` when there is no separator, or when a quoted table is not immediately
/// followed by one. A table whose quote is never closed falls back to splitting on the first
/// colon, which keeps unquoted input containing a stray backtick parsing as before.
fn split_table_key(s: &str) -> Option<(&str, &str)> {
	if !s.starts_with('`') {
		return s.split_once(':');
	}
	let mut chars = s.char_indices().skip(1);
	while let Some((i, c)) = chars.next() {
		match c {
			'\\' => {
				chars.next();
			}
			'`' => {
				let table_end = i + '`'.len_utf8();
				return Some((&s[..table_end], s[table_end..].strip_prefix(':')?));
			}
			_ => {}
		}
	}
	s.split_once(':')
}

impl ToSql for RecordId {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		use crate::utils::escape::EscapeSqonIdent;
		write_sql!(f, fmt, "{}:{}", EscapeSqonIdent(self.table.as_str()), self.key);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_simple_roundtrips_escaped_string_key() {
		let id1 = RecordId::new("person", "needs escaping");
		let id2 = RecordId::parse_simple(&id1.to_sql()).unwrap();
		assert_eq!(id1, id2);
		let id3 = RecordId::parse_simple(&id2.to_sql()).unwrap();
		assert_eq!(id1, id3);
	}

	#[test]
	fn parse_simple_roundtrips_key_with_backticks() {
		let id = RecordId::new("person", "he said `hi`");
		assert_eq!(RecordId::parse_simple(&id.to_sql()).unwrap(), id);
	}

	#[test]
	fn parse_simple_roundtrips_escaped_table() {
		let id = RecordId::new("weird table", "tobie");
		assert_eq!(RecordId::parse_simple(&id.to_sql()).unwrap(), id);
	}

	#[test]
	fn parse_simple_roundtrips_colons_in_both_fragments() {
		let id = RecordId::new("we:ird", "tobie");
		assert_eq!(id.to_sql(), "`we:ird`:tobie");
		assert_eq!(RecordId::parse_simple(&id.to_sql()).unwrap(), id);

		let id = RecordId::new("we:ird", "to:bie");
		assert_eq!(RecordId::parse_simple(&id.to_sql()).unwrap(), id);

		// An escaped backtick inside the table must not be read as its closing quote.
		let id = RecordId::new("we`:ird", "tobie");
		assert_eq!(RecordId::parse_simple(&id.to_sql()).unwrap(), id);
	}

	#[test]
	fn parse_simple_rejects_quoted_table_without_separator() {
		assert!(RecordId::parse_simple("`weird`").is_err());
		assert!(RecordId::parse_simple("`weird`tobie").is_err());
	}

	#[test]
	fn parse_simple_keeps_unwrapped_spaces() {
		// Still accepts a dumb split without SurrealQL escaping.
		let id = RecordId::parse_simple("person:needs escaping").unwrap();
		assert_eq!(id, RecordId::new("person", "needs escaping"));
	}

	#[test]
	fn parse_simple_always_produces_a_string_key() {
		// The key is not re-typed, so a non-string key does not survive the roundtrip.
		let id = RecordId::new("person", 1i64);
		assert_eq!(id.to_sql(), "person:1");
		assert_eq!(RecordId::parse_simple(&id.to_sql()).unwrap(), RecordId::new("person", "1"));
	}

	#[test]
	fn parse_simple_plain_id() {
		let id = RecordId::parse_simple("person:tobie").unwrap();
		assert_eq!(id, RecordId::new("person", "tobie"));
		assert_eq!(RecordId::parse_simple(&id.to_sql()).unwrap(), id);
	}
}
