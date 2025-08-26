/// Record id key types
pub mod key;
/// Record id range types
pub mod range;

pub use key::*;
pub use range::*;
use serde::{Deserialize, Serialize};

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
	pub fn new<T: Into<String>, K>(table: T, key: K) -> Self
	where
		RecordIdKey: From<K>,
	{
		RecordId {
			table: table.into(),
			key: key.into(),
		}
	}
}
