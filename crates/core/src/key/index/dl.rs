//! Stores the doc length
//!
//! This key is used in the concurrent full-text search implementation to store
//! the length of individual documents in the index. Document length is a critical
//! factor in relevance scoring algorithms like BM25, which normalize term frequencies
//! based on document length.
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
use crate::idx::docids::DocId;
use crate::idx::ft::DocLength;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::impl_key;
use crate::kvs::KVKey;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) struct Dl<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	_f: u8,
	_g: u8,
	pub id: DocId,
}
impl_key!(Dl<'a>);

impl KVKey for Dl<'_> {
	type ValueType = DocLength;
}

impl Categorise for Dl<'_> {
	fn categorise(&self) -> Category {
		Category::IndexDocLength
	}
}

impl<'a> Dl<'a> {
	/// Creates a new document length key
	///
	/// This constructor creates a key that stores the length of an individual document
	/// in the full-text index. Document length is a critical factor in relevance scoring
	/// algorithms like BM25, which normalize term frequencies based on document length.
	///
	/// # Arguments
	/// * `ns` - Namespace identifier
	/// * `db` - Database identifier
	/// * `tb` - Table identifier
	/// * `ix` - Index identifier
	/// * `id` - The document ID whose length is being stored
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, id: DocId) -> Self {
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
			_f: b'd',
			_g: b'l',
			id,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::{KeyDecode, KeyEncode};

	#[test]
	fn key() {
		use super::*;
		let val = Dl::new("testns", "testdb", "testtb", "testix", 16);
		let enc = Dl::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!dl\0\0\0\0\0\0\0\x10");

		let dec = Dl::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
