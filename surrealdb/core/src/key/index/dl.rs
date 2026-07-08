//! Stores the doc length
//!
//! This key is used in the concurrent full-text search implementation to store
//! the length of individual documents in the index. Document length is a
//! critical factor in relevance scoring algorithms like BM25, which normalize
//! term frequencies based on document length.
//!
//! The key structure includes:
//! - Namespace, database, table, and index identifiers
//! - Document ID
//!
//! This key is essential for:
//! - Calculating accurate relevance scores for search results
//! - Supporting document length normalization
//! - Enabling proper ranking of search results based on term frequency and document length
//! - Providing document-specific statistics for the full-text search engine
use std::borrow::Cow;

use crate::catalog::IndexId;
use crate::idx::ft::DocLength;
use crate::idx::seqdocids::DocId;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};
use crate::val::TableName;

key! {
	/// Document length key
	///
	/// A key that stores the length of an individual
	/// document in the full-text index. Document length is a critical factor
	/// in relevance scoring algorithms like BM25, which normalize term
	/// frequencies based on document length.
	///
	/// # Arguments
	/// * `ns` - Namespace identifier
	/// * `db` - Database identifier
	/// * `tb` - Table identifier
	/// * `ix` - Index identifier
	/// * `id` - The document ID whose length is being stored
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
		pub(crate) struct Dl<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'd',
		b'l',
		pub id: DocId,
	}
}

impl_kv_key_storekey!(Dl<'a> => DocLength);

impl Categorise for Dl<'_> {
	fn categorise(&self) -> Category {
		Category::IndexDocLength
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
		let val = Dl {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			id: 16,
		};
		let enc = Dl::encode_key(&val).unwrap();
		assert_eq!(
			enc.as_slice(),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!dl\0\0\0\0\0\0\0\x10"
		);
	}
}
