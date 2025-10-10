/// Record id key types
pub mod key;
/// Record id range types
pub mod range;

pub use key::*;
pub use range::*;
use serde::{Deserialize, Serialize};

use crate::sql::ToSqon;

/// Represents a record identifier in SurrealDB
///
/// A record identifier consists of a table name and a key that uniquely identifies
/// a record within that table. This is the primary way to reference specific records.

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

impl ToSqon for RecordId {
	fn fmt_sqon(&self, f: &mut String) {
		f.push_str(&self.table);
		f.push(':');
		self.key.fmt_sqon(f);
	}
}
