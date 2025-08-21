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

use anyhow::Result;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::idx::docids::DocId;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Tt<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	_f: u8,
	_g: u8,
	pub term: &'a str,
	pub doc_id: DocId,
	#[serde(with = "uuid::serde::compact")]
	pub nid: Uuid,
	#[serde(with = "uuid::serde::compact")]
	pub uid: Uuid,
	pub add: bool,
}

impl KVKey for Tt<'_> {
	type ValueType = String;
}

impl Categorise for Tt<'_> {
	fn categorise(&self) -> Category {
		Category::IndexTermDocuments
	}
}

impl<'a> Tt<'a> {
	/// Creates a new term-document key
	///
	/// This constructor creates a key that represents a term occurrence in a
	/// document. It's used by the full-text search engine to build the
	/// inverted index that maps terms to the documents containing them.
	///
	/// # Arguments
	/// * `ns` - Namespace identifier
	/// * `db` - Database identifier
	/// * `tb` - Table identifier
	/// * `ix` - Index identifier
	/// * `term` - The term being indexed
	/// * `doc_id` - The document ID where the term appears
	/// * `nid` - Node ID for distributed transaction tracking
	/// * `uid` - Transaction ID for concurrency control
	/// * `add` - Whether this is an addition (true) or removal (false) operation
	#[expect(clippy::too_many_arguments)]
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		ix: &'a str,
		term: &'a str,
		doc_id: DocId,
		nid: Uuid,
		uid: Uuid,
		add: bool,
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
			_g: b't',
			term,
			doc_id,
			nid,
			uid,
			add,
		}
	}

	/// Creates a key range for querying a specific term
	///
	/// This method generates a key range that can be used to query all
	/// occurrences of a specific term across all documents in the full-text
	/// index. It's used for term-specific searches and frequency analysis.
	///
	/// # Arguments
	/// * `ns` - Namespace identifier
	/// * `db` - Database identifier
	/// * `tb` - Table identifier
	/// * `ix` - Index identifier
	/// * `term` - The specific term to query
	///
	/// # Returns
	/// A tuple of (start, end) keys that define the range for database queries
	pub(crate) fn term_range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		ix: &'a str,
		term: &'a str,
	) -> Result<(Vec<u8>, Vec<u8>)> {
		let prefix = TtTermPrefix::new(ns, db, tb, ix, term);
		let mut beg = prefix.encode_key()?;
		beg.extend([0; 41]);
		let mut end = prefix.encode_key()?;
		end.extend([255; 41]);
		Ok((beg, end))
	}

	/// Creates a key range for querying all terms in an index
	///
	/// This method generates a key range that can be used to query all terms
	/// in the full-text index. It's used for operations that need to scan
	/// all indexed terms, such as index maintenance, compaction, or complete
	/// index scans.
	///
	/// # Arguments
	/// * `ns` - Namespace identifier
	/// * `db` - Database identifier
	/// * `tb` - Table identifier
	/// * `ix` - Index identifier
	///
	/// # Returns
	/// A tuple of (start, end) keys that define the range for database queries
	pub(crate) fn terms_range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		ix: &'a str,
	) -> Result<(Vec<u8>, Vec<u8>)> {
		let prefix = TtTermsPrefix::new(ns, db, tb, ix);
		let mut beg = prefix.encode_key()?;
		beg.push(0);
		let mut end = prefix.encode_key()?;
		end.push(255);
		Ok((beg, end))
	}

	pub fn decode_key(k: &[u8]) -> Result<Tt<'_>> {
		Ok(storekey::deserialize(k)?)
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
struct TtTermPrefix<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	_f: u8,
	_g: u8,
	pub term: &'a str,
}

impl KVKey for TtTermPrefix<'_> {
	type ValueType = String;
}

impl<'a> TtTermPrefix<'a> {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: &'a str, term: &'a str) -> Self {
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
			_g: b't',
			term,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
struct TtTermsPrefix<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	_f: u8,
	_g: u8,
}

impl KVKey for TtTermsPrefix<'_> {
	type ValueType = String;
}

impl<'a> TtTermsPrefix<'a> {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: &'a str) -> Self {
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
			_g: b't',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		let val = Tt::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testix",
			"term",
			129,
			Uuid::from_u128(1),
			Uuid::from_u128(2),
			true,
		);
		let enc = Tt::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!ttterm\0\0\0\0\0\0\0\0\x81\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02\x01");
	}

	#[test]
	fn term_range() {
		let (beg, end) =
			Tt::term_range(NamespaceId(1), DatabaseId(2), "testtb", "testix", "term").unwrap();
		assert_eq!(beg, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!ttterm\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
		assert_eq!(
			end,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!ttterm\0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
		);
	}

	#[test]
	fn terms_range() {
		let (beg, end) =
			Tt::terms_range(NamespaceId(1), DatabaseId(2), "testtb", "testix").unwrap();
		assert_eq!(beg, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!tt\0");
		assert_eq!(end, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!tt\xff");
	}
}
