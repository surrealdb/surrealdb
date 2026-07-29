//! Stores the term/document frequency and offsets
//!
//! This key is used in the concurrent full-text search implementation to store
//! term-document relationships. It maps terms to document IDs, allowing for
//! efficient lookup of documents containing specific terms.
//!
//! The key structure includes:
//! - Namespace, database, table, and index identifiers
//! - The term being indexed
//! - An optional document ID
//!
//! This key is essential for:
//! - Quickly finding documents that contain specific terms
//! - Supporting term-based document retrieval
//! - Enabling efficient text search operations

use std::borrow::Cow;

use roaring::RoaringTreemap;
use surrealdb_strand::TableName;

use crate::catalog::IndexId;
use crate::idx::docids::DocId;
use crate::idx::ft::fulltext::TermDocument;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
		pub(crate) struct TdRoot<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b't',
		b'd',
		pub term: Cow<'a, str>,
	}
}

impl_kv_key_storekey!(TdRoot<'a> => RoaringTreemap);

impl Categorise for TdRoot<'_> {
	fn categorise(&self) -> Category {
		Category::IndexTermDocument
	}
}

key! {
	/// Term-document mapping key
	///
	/// A key that maps a term to a document ID.
	/// It's used by the full-text search engine to efficiently find documents
	/// that contain specific terms during search operations.
	///
	/// # Fields
	/// * `ns` - Namespace identifier
	/// * `db` - Database identifier
	/// * `tb` - Table identifier
	/// * `ix` - Index identifier
	/// * `term` - The term being indexed
	/// * `id` - Optional document ID (Some for specific document, None for term prefix)
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Td<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b't',
		b'd',
		pub term: Cow<'a, str>,
		pub id: DocId,
	}
}

impl_kv_key_storekey!(Td<'a> => TermDocument);

impl Categorise for Td<'_> {
	fn categorise(&self) -> Category {
		Category::IndexTermDocument
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn root() {
		let tb = TableName::from("testtb");
		let val = TdRoot {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			term: "term".into(),
		};
		let enc = TdRoot::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!tdterm\0");
	}

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = Td {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			term: "term".into(),
			id: 129,
		};
		let enc = Td::encode_key(&val).unwrap();
		assert_eq!(
			&*enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!tdterm\0\0\0\0\0\0\0\0\x81"
		);
	}
}
