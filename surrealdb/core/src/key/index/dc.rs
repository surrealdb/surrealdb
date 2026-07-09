//! Stores the term/document frequency and offsets for a document
//!
//! This key is used in the concurrent full-text search implementation to store
//! document count and length information. It tracks statistics about documents
//! in the full-text index, which are essential for relevance scoring algorithms
//! like BM25.
//!
//! The key structure includes:
//! - Namespace, database, table, and index identifiers
//! - Document ID
//! - Transaction IDs (nid, uid) for concurrency control
//!
//! This key is essential for:
//! - Maintaining document statistics for scoring calculations
//! - Supporting document length normalization in search results
//! - Enabling efficient compaction of index data
//! - Providing accurate document count information for the index
use std::borrow::Cow;

use anyhow::Result;
use uuid::Uuid;

use crate::catalog::IndexId;
use crate::idx::docids::DocId;
use crate::idx::ft::fulltext::DocLengthAndCount;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::val::TableName;

key! {
	/// Document count and length key
	///
	/// a key that represents document statistics for
	/// the full-text index. It's used to track document count and length
	/// information, which is essential for relevance scoring algorithms like
	/// BM25.
	///
	/// # Fields
	/// * `ns` - Namespace identifier
	/// * `db` - Database identifier
	/// * `tb` - Table identifier
	/// * `ix` - Index identifier
	/// * `doc_id` - The document ID being tracked
	/// * `nid` - Node ID for distributed transaction tracking
	/// * `uid` - Transaction ID for concurrency control
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Dc<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'd',
		b'c',
		pub doc_id: DocId,
		pub nid: Uuid,
		pub uid: Uuid,
	}
}

impl_kv_key_storekey!(Dc<'a> => DocLengthAndCount);

impl Categorise for Dc<'_> {
	fn categorise(&self) -> Category {
		Category::IndexFullTextDocCountAndLength
	}
}

key! {
	/// The prefix for full text document count and length entries.
	///
	/// This key is both a prefix and an actual key.
	/// The 'real' value is stored under the prefix, uncompected delta values are stored in their
	/// own key with this key as the prefix.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct DcPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'd',
		b'c',
	}
}
impl_kv_range_storekey!(DcPrefix<'_>);
impl_kv_key_storekey!(DcPrefix<'a> => DocLengthAndCount);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key_with_ids() {
		let tb = TableName::from("testtb");
		let val = Dc {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			doc_id: 129,
			nid: Uuid::from_u128(1),
			uid: Uuid::from_u128(2),
		};
		let enc = Dc::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!dc\0\0\0\0\0\0\0\x81\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02");
	}
}
