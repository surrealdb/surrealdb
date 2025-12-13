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
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::ft::fulltext::TermDocument;
use crate::idx::seqdocids::DocId;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct TdRoot<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
	pub term: Cow<'a, str>,
}

impl_kv_key_storekey!(TdRoot<'_> => RoaringTreemap);

impl Categorise for TdRoot<'_> {
	fn categorise(&self) -> Category {
		Category::IndexTermDocument
	}
}

impl<'a> TdRoot<'a> {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		term: &'a str,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'+',
			ix,
			_e: b'!',
			_f: b't',
			_g: b'd',
			term: Cow::Borrowed(term),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Td<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
	pub term: Cow<'a, str>,
	pub id: DocId,
}

impl_kv_key_storekey!(Td<'_> => TermDocument);

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
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		term: &'a str,
		id: DocId,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'+',
			ix,
			_e: b'!',
			_f: b't',
			_g: b'd',
			term: Cow::Borrowed(term),
			id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn root() {
		let val = TdRoot::new(NamespaceId(1), DatabaseId(2), "testtb", IndexId(3), "term");
		let enc = TdRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!tdterm\0");
	}

	#[test]
	fn key() {
		let val = Td::new(NamespaceId(1), DatabaseId(2), "testtb", IndexId(3), "term", 129);
		let enc = Td::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!tdterm\0\0\0\0\0\0\0\0\x81"
		);
	}
}
