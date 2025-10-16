//! Stores doc keys for doc_ids
use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::seqdocids::DocId;
use crate::kvs::impl_kv_key_storekey;
use crate::val::RecordIdKey;

/// Id inverted. DocId -> Id
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct InvertedIdKey<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
	_d: u8,
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
	pub id: DocId,
}

impl_kv_key_storekey!(InvertedIdKey<'_> => RecordIdKey);

impl<'a> InvertedIdKey<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: IndexId, id: DocId) -> Self {
		InvertedIdKey {
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
			_f: b'i',
			_g: b'i',
			id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = InvertedIdKey::new(NamespaceId(1), DatabaseId(2), "testtb", IndexId(3), 1);
		let enc = InvertedIdKey::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!ii\0\0\0\0\0\0\0\x01"
		);
	}
}
