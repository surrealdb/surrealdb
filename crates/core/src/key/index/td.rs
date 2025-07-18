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

use crate::idx::docids::DocId;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Td<'a> {
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
	pub term: &'a str,
	pub id: Option<DocId>,
}
impl_key!(Td<'a>);

impl Categorise for Td<'_> {
	fn categorise(&self) -> Category {
		Category::IndexTermDocument
	}
}

impl<'a> Td<'a> {
	/// Creates a new term-document mapping key
	///
	/// This constructor creates a key that maps a term to a document ID.
	/// It's used by the full-text search engine to efficiently find documents
	/// that contain specific terms during search operations.
	///
	/// # Arguments
	/// * `ns` - Namespace identifier
	/// * `db` - Database identifier
	/// * `tb` - Table identifier
	/// * `ix` - Index identifier
	/// * `term` - The term being indexed
	/// * `id` - Optional document ID (Some for specific document, None for term prefix)
	pub(crate) fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		term: &'a str,
		id: Option<DocId>,
	) -> Self {
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
			_f: b't',
			_g: b'd',
			term,
			id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::{KeyDecode, KeyEncode};

	#[test]
	fn key() {
		let val = Td::new("testns", "testdb", "testtb", "testix", "term", Some(129));
		let enc = Td::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!tdterm\0\x01\0\0\0\0\0\0\0\x81");
		let dec = Td::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
