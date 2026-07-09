//! Stores the term/document frequency and offsets
//!
//! This key is used in the concurrent full-text search implementation to store
//! term-document relationships with their frequencies and offsets. It maps
//! terms to the documents that contain them, allowing for efficient text search
//! operations.
//!
//! The key structure includes:
//! - Namespace, database, table, and index identifiers
//! - The term being indexed
//! - The document ID where the term appears
//! - Transaction IDs (nid, uid) for concurrency control
//! - A flag indicating whether this is an addition or removal
//!
//! This key is essential for:
//! - Building the inverted index that maps terms to documents
//! - Supporting concurrent read and write operations
//! - Enabling efficient term frequency tracking for relevance scoring
use std::borrow::Cow;

use anyhow::Result;
use uuid::Uuid;

use crate::catalog::IndexId;
use crate::idx::docids::DocId;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::val::TableName;

key! {
	/// Term-document key
	///
	/// A key that represents a term occurrence in a
	/// document. It's used by the full-text search engine to build the
	/// inverted index that maps terms to the documents containing them.
	///
	/// # Fields
	/// * `ns` - Namespace identifier
	/// * `db` - Database identifier
	/// * `tb` - Table identifier
	/// * `ix` - Index identifier
	/// * `term` - The term being indexed
	/// * `doc_id` - The document ID where the term appears
	/// * `nid` - Node ID for distributed transaction tracking
	/// * `uid` - Transaction ID for concurrency control
	/// * `add` - Whether this is an addition (true) or removal (false) operation
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Tt<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b't',
		b't',
		pub term: Cow<'a, str>,
		pub doc_id: DocId,
		pub nid: Uuid,
		pub uid: Uuid,
		pub add: bool,
	}
}

impl_kv_key_storekey!(Tt<'a> => String);

impl Categorise for Tt<'_> {
	fn categorise(&self) -> Category {
		Category::IndexTermDocuments
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub struct TtTermPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b't',
		b't',
		pub term: Cow<'a, str>,
	}
}

impl_kv_key_storekey!(TtTermPrefix<'a> => String);
impl_kv_range_storekey!(TtTermPrefix<'_>);

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub struct TtTermsPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b't',
		b't',
	}
}

impl_kv_key_storekey!(TtTermsPrefix<'a> => String);
impl_kv_range_storekey!(TtTermsPrefix<'_>);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = Tt {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			term: Cow::Borrowed("term"),
			doc_id: 129,
			nid: Uuid::from_u128(1),
			uid: Uuid::from_u128(2),
			add: true,
		};
		let enc = Tt::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!ttterm\0\0\0\0\0\0\0\0\x81\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02\x03");
	}
}
