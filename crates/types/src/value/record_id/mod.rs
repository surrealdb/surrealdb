/// Record id key types
pub mod key;
/// Record id range types
pub mod range;

use std::fmt;

pub use key::*;
pub use range::*;
use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::sql::ToSql;
use crate::utils::escape::EscapeRid;

/// Represents a record identifier in SurrealDB
///
/// A record identifier consists of a table name and a key that uniquely identifies
/// a record within that table. This is the primary way to reference specific records.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct RecordId {
	/// The name of the table containing the record
	pub table: String,
	/// The key that uniquely identifies the record within the table
	pub key: RecordIdKey,
}

impl RecordId {
	/// Creates a new record id from the given table and key
	pub fn new(table: impl Into<String>, key: impl Into<RecordIdKey>) -> Self {
		RecordId {
			table: table.into(),
			key: key.into(),
		}
	}

	/// Checks if the record id is of the specified type.
	pub fn is_record_type(&self, val: &[String]) -> bool {
		val.is_empty() || val.contains(&self.table)
	}

	/// Parses a record id which must be in the format of `table:key`.
	pub fn parse_simple(s: &str) -> anyhow::Result<Self> {
		let (table, key) =
			s.split_once(':').ok_or_else(|| anyhow::anyhow!("Invalid record id: {s}"))?;
		Ok(Self::new(table, key))
	}
}

impl fmt::Display for RecordId {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", EscapeRid(&self.table), self.key)
	}
}

impl ToSql for RecordId {
	fn fmt_sql(&self, f: &mut String) {
		f.push_str(&format!("{}:{}", self.table, self.key))
	}
}
