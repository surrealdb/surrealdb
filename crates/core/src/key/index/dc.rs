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

use crate::idx::docids::DocId;
use crate::idx::ft::fulltext::DocLengthAndCount;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::KVKey;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Dc<'a> {
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
	pub doc_id: DocId,
	#[serde(with = "uuid::serde::compact")]
	pub nid: Uuid,
	#[serde(with = "uuid::serde::compact")]
	pub uid: Uuid,
}

impl KVKey for Dc<'_> {
	type ValueType = DocLengthAndCount;
}

impl Categorise for Dc<'_> {
	fn categorise(&self) -> Category {
		Category::IndexFullTextDocCountAndLength
	}
}

impl<'a> Dc<'a> {
	/// Creates a new document count and length key
	///
	/// This constructor creates a key that represents document statistics for the full-text index.
	/// It's used to track document count and length information, which is essential for
	/// relevance scoring algorithms like BM25.
	///
	/// # Arguments
	/// * `ns` - Namespace identifier
	/// * `db` - Database identifier
	/// * `tb` - Table identifier
	/// * `ix` - Index identifier
	/// * `doc_id` - The document ID being tracked
	/// * `nid` - Node ID for distributed transaction tracking
	/// * `uid` - Transaction ID for concurrency control
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
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
			tb,
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
	/// document count and total length information needed for scoring calculations.
	///
	/// # Arguments
	/// * `ns` - Namespace identifier
	/// * `db` - Database identifier
	/// * `tb` - Table identifier
	/// * `ix` - Index identifier
	///
	/// # Returns
	/// The encoded root key as a byte vector
	pub(crate) fn new_root(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str) -> Result<Vec<u8>> {
		DcPrefix::new(ns, db, tb, ix).encode_key()
	}

	/// Creates a key range for querying document count and length statistics
	///
	/// This method generates a key range that can be used to query all document
	/// count and length statistics for a specific index. It's used for operations
	/// like compaction, scoring calculations, and index maintenance.
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
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
	) -> Result<(Vec<u8>, Vec<u8>)> {
		let prefix = DcPrefix::new(ns, db, tb, ix);
		let mut beg = prefix.encode_key()?;
		beg.extend([0; 40]);
		let mut end = prefix.encode_key()?;
		end.extend([255; 40]);
		Ok((beg, end))
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
struct DcPrefix<'a> {
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
}

impl KVKey for DcPrefix<'_> {
	type ValueType = Vec<u8>;
}

impl<'a> DcPrefix<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str) -> Self {
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
			_g: b'c',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key_with_ids() {
		let val = Dc::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			129,
			Uuid::from_u128(1),
			Uuid::from_u128(2),
		);
		let enc = Dc::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!dc\0\0\0\0\0\0\0\x81\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02");
	}

	#[test]
	fn key_root() {
		let enc = Dc::new_root("testns", "testdb", "testtb", "testix").unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!dc");
	}

	#[test]
	fn range() {
		let (beg, end) = Dc::range("testns", "testdb", "testtb", "testix").unwrap();
		println!("{} {}", beg.len(), end.len());
		assert_eq!(beg, b"/*testns\0*testdb\0*testtb\0+testix\0!dc\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
		assert_eq!(
			end,
			b"/*testns\0*testdb\0*testtb\0+testix\0!dc\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
		);
	}
}
