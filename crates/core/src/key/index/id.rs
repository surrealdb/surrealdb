//! Document ID Mapping Key (`Id`) for Full-Text Index
//!
//! The `Id` key stores the mapping between SurrealDB record IDs (`Thing`) and
//! internal numeric document IDs (`DocId`) used by the full-text search engine.
//!
//! ## Key Structure
//! ```no_compile
//! /*{namespace}*{database}*{table}+{index}!id{record_id}
//! ```
//!
//! ## Purpose
//! - **ID Translation**: Converts between user-facing record IDs and internal numeric document IDs
//! - **Bidirectional Mapping**: Works with `Bi` keys to provide reverse lookups
//! - **Index Efficiency**: Numeric document IDs are more efficient for internal search operations
//!
//! ## Usage in Full-Text Search
//! The `Id` key is essential for the full-text search pipeline:
//! 1. **Indexing Phase**: Record IDs are converted to document IDs using `Id` keys
//! 2. **Search Phase**: Results use document IDs internally for efficiency
//! 3. **Result Retrieval**: Document IDs are converted back to record IDs for user presentation
//!
//! ## Category
//! - **Category**: `IndexInvertedDocIds`
//! - **Domain**: Full-text search document ID mapping
//!
//! ## Integration with Document ID Lifecycle
//! 1. **ID Resolution**: When a document is indexed, its record ID is mapped to a numeric document
//!    ID
//! 2. **Storage**: The `Id` key stores: `record_id → doc_id`
//! 3. **Allocation**: If no mapping exists, a new document ID is allocated from the sequence (using
//!    `Ib` keys)
//! 4. **Reverse Mapping**: A complementary `Bi` key stores: `doc_id → record_id`
//!
//! ## Performance Characteristics
//! - **Space Efficient**: Numeric document IDs are smaller than full record IDs
//! - **Cache Friendly**: Sequential numeric IDs improve cache locality
//! - **Concurrent Safe**: Works with distributed sequence mechanism to prevent ID conflicts
//! - **Scalable**: Efficient lookups scale with the number of indexed documents
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::idx::docids::DocId;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use crate::val::RecordIdKey;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Id<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	_f: u8,
	_g: u8,
	pub id: RecordIdKey,
}

impl KVKey for Id<'_> {
	type ValueType = DocId;
}

impl Categorise for Id<'_> {
	fn categorise(&self) -> Category {
		Category::IndexInvertedDocIds
	}
}

impl<'a> Id<'a> {
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: &'a str, id: RecordIdKey) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'+',
			ix,
			_e: b'!',
			_f: b'i',
			_g: b'd',
			id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		let val = Id::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testix",
			RecordIdKey::from(strand!("id").to_owned()),
		);
		let enc = Id::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!id\0\0\0\x01id\0",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
