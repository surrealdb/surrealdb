//! Stores encoded document lengths in chunks.
//!
//! Each chunk stores 4096 SmallFloat-encoded document lengths (4KB).
//! This enables batch loading of document lengths with a single range query.

use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::seqdocids::DocId;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

/// Number of documents per chunk (4KB with 1 byte per doc)
pub const CHUNK_SIZE: u64 = 4096;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Dle<'a> {
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
	_h: u8,
	pub chunk_id: u64,
}

impl_kv_key_storekey!(Dle<'_> => Vec<u8>);

impl Categorise for Dle<'_> {
	fn categorise(&self) -> Category {
		Category::IndexDocLengthEncoded
	}
}

impl<'a> Dle<'a> {
	/// Creates a new encoded document length chunk key.
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		chunk_id: u64,
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
			_g: b'l',
			_h: b'e', // 'e' for encoded (differs from dl.rs which uses just 'dl')
			chunk_id,
		}
	}

	/// Returns the chunk ID for a given document ID.
	#[inline]
	pub fn chunk_id(doc_id: DocId) -> u64 {
		doc_id / CHUNK_SIZE
	}

	/// Returns the offset within a chunk for a given document ID.
	#[inline]
	pub fn offset(doc_id: DocId) -> usize {
		(doc_id % CHUNK_SIZE) as usize
	}

	/// Decodes a key from bytes.
	#[allow(dead_code)] // Reserved for future batch loading optimization
	pub fn decode_key(k: &[u8]) -> anyhow::Result<Dle<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = Dle::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), 0);
		let enc = Dle::encode_key(&val).unwrap();
		// Key should start with /*
		assert!(enc.starts_with(b"/*"));
	}

	#[test]
	fn chunk_id_from_doc_id() {
		// 4096 docs per chunk
		assert_eq!(Dle::chunk_id(0), 0);
		assert_eq!(Dle::chunk_id(4095), 0);
		assert_eq!(Dle::chunk_id(4096), 1);
		assert_eq!(Dle::chunk_id(8191), 1);
		assert_eq!(Dle::chunk_id(8192), 2);
	}

	#[test]
	fn offset_in_chunk() {
		assert_eq!(Dle::offset(0), 0);
		assert_eq!(Dle::offset(100), 100);
		assert_eq!(Dle::offset(4095), 4095);
		assert_eq!(Dle::offset(4096), 0);
		assert_eq!(Dle::offset(4097), 1);
	}
}
