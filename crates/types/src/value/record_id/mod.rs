/// Record id key types
pub mod key;
/// Record id range types
pub mod range;

pub use key::*;
pub use range::*;
use serde::{Deserialize, Serialize};

use crate::sql::{SqlFormat, ToSql};
use crate::utils::escape::EscapeSqonIdent;
use crate::{Table, write_sql};

/// Represents a record identifier in SurrealDB
///
/// A record identifier consists of a table name and a key that uniquely identifies
/// a record within that table. This is the primary way to reference specific records.

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
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
	pub fn is_table_type(&self, tables: &[String]) -> bool {
		tables.is_empty() || tables.contains(&self.table)
	}

	/// Parses a record id which must be in the format of `table:key`.
	pub fn parse_simple(s: &str) -> anyhow::Result<Self> {
		let (table, key) =
			s.split_once(':').ok_or_else(|| anyhow::anyhow!("Invalid record id: {s}"))?;
		Ok(Self::new(table, key))
	}
}

impl ToSql for RecordId {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, "{}:", EscapeSqonIdent(self.table.as_str()));
		self.key.fmt_sql(f, fmt);
	}
}
