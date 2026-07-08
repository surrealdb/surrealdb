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
use std::borrow::Cow;
use std::fmt::Debug;

use crate::catalog::IndexId;
use crate::idx::seqdocids::DocId;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{KVKey, KVKeyDecode, key};
use crate::val::{IndexFormat, RecordIdKey, TableName};

key! {
	#[derive(Debug, Clone, PartialEq)]
	pub(crate) struct Id<'a> for IndexFormat{
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'i',
		b'd',
		pub id: Cow<'a,RecordIdKey>,
	}
}

impl KVKey for Id<'_> {
	type Value = DocId;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> anyhow::Result<()> {
		storekey::encode_format::<IndexFormat, _, _>(buffer, self)
			.map_err(|_| crate::err::Error::Unencodable)?;
		Ok(())
	}

	fn value_context(&self) {}
}
impl<'a> KVKeyDecode<'a> for Id<'a> {
	fn decode_key(bytes: &'a [u8]) -> anyhow::Result<Self> {
		Ok(storekey::decode_borrow_format::<IndexFormat, _>(bytes).map_err(|_| {
			crate::err::Error::Corrupted("Document ID mapping key cannot be decoded")
		})?)
	}
}

impl Categorise for Id<'_> {
	fn categorise(&self) -> Category {
		Category::IndexInvertedDocIds
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = Id {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			id: Cow::Owned(RecordIdKey::from("id".to_owned())),
		};
		let enc = Id::encode_key(&val).unwrap();
		assert_eq!(
			&*enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!id\x03id\0",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
