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
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::ft::fulltext::DocLengthAndCount;
use crate::idx::seqdocids::DocId;
use crate::key::category::{Categorise, Category};
use crate::kvs::{KVKey, impl_kv_key_storekey};
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Dc<'a> {
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
	pub doc_id: DocId,
	pub nid: Uuid,
	pub uid: Uuid,
}

impl_kv_key_storekey!(Dc<'_> => DocLengthAndCount);

impl Categorise for Dc<'_> {
	fn categorise(&self) -> Category {
		Category::IndexFullTextDocCountAndLength
	}
}

impl<'a> Dc<'a> {
	/// Creates a new document count and length key
	///
	/// This constructor creates a key that represents document statistics for
	/// the full-text index. It's used to track document count and length
	/// information, which is essential for relevance scoring algorithms like
	/// BM25.
	///
	/// # Arguments
	/// * `ns` - Namespace identifier
	/// * `db` - Database identifier
	/// * `tb` - Table identifier
	/// * `ix` - Index identifier
	/// * `doc_id` - The document ID being tracked
	/// * `nid` - Node ID for distributed transaction tracking
	/// * `uid` - Transaction ID for concurrency control
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		doc_id: DocId,
		nid: Uuid,
		uid: Uuid,
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
			_f: b'd',
			_g: b'c',
			doc_id,
			nid,
			uid,
		}
	}

	/// Creates a root key for document count and length statistics
	///
	/// This method generates a root key that serves as the base for storing
	/// aggregated document statistics. It's used for maintaining the overall
	/// document count and total length information needed for scoring
	/// calculations.
	///
	/// # Arguments
	/// * `ns` - Namespace identifier
	/// * `db` - Database identifier
	/// * `tb` - Table identifier
	/// * `ix` - Index identifier
	///
	/// # Returns
	/// The encoded root key as a byte vector
	pub(crate) fn new_root(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
	) -> Result<Vec<u8>> {
		DcPrefix::new(ns, db, tb, ix).encode_key()
	}

	/// Creates a key range for querying document count and length statistics
	///
	/// This method generates a key range that can be used to query all document
	/// count and length statistics for a specific index. It's used for
	/// operations like compaction, scoring calculations, and index
	/// maintenance.
	///
	/// # Arguments
	/// * `ns` - Namespace identifier
	/// * `db` - Database identifier
	/// * `tb` - Table identifier
	/// * `ix` - Index identifier
	///
	/// # Returns
	/// A tuple of (start, end) keys that define the range for database queries
	pub(crate) fn range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
	) -> Result<(Vec<u8>, Vec<u8>)> {
		let prefix = DcPrefix::new(ns, db, tb, ix);
		let mut beg = prefix.encode_key()?;
		beg.extend([0; 40]);
		let mut end = prefix.encode_key()?;
		end.extend([255; 40]);
		Ok((beg, end))
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
struct DcPrefix<'a> {
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
}

impl_kv_key_storekey!(DcPrefix<'_> => Vec<u8>);

impl<'a> DcPrefix<'a> {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, ix: IndexId) -> Self {
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
			_f: b'd',
			_g: b'c',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key_with_ids() {
		let tb = TableName::from("testtb");
		let val = Dc::new(
			NamespaceId(1),
			DatabaseId(2),
			&tb,
			IndexId(3),
			129,
			Uuid::from_u128(1),
			Uuid::from_u128(2),
		);
		let enc = Dc::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!dc\0\0\0\0\0\0\0\x81\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02");
	}

	#[test]
	fn key_root() {
		let tb = TableName::from("testtb");
		let enc = Dc::new_root(NamespaceId(1), DatabaseId(2), &tb, IndexId(3)).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!dc");
	}

	#[test]
	fn range() {
		let tb = TableName::from("testtb");
		let (beg, end) = Dc::range(NamespaceId(1), DatabaseId(2), &tb, IndexId(3)).unwrap();
		assert_eq!(beg, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!dc\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
		assert_eq!(
			end,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!dc\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
		);
	}
}
