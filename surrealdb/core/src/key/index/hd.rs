//! Stores the DocIds -> Thing of an HNSW index
use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::seqdocids::DocId;
use crate::idx::trees::hnsw::docs::HnswDocsState;
use crate::kvs::impl_kv_key_storekey;
use crate::val::{RecordIdKey, TableName};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct HdRoot<'a> {
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

impl_kv_key_storekey!(HdRoot<'_> => HnswDocsState);

impl<'a> HdRoot<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, ix: IndexId) -> Self {
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
			_f: b'h',
			_g: b'd',
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Hd<'a> {
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
}

impl_kv_key_storekey!(Hd<'_> => RecordIdKey);

impl<'a> Hd<'a> {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		doc_id: DocId,
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
			_f: b'h',
			_g: b'd',
			doc_id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn root() {
		let tb = TableName::from("testtb");
		let val = HdRoot::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3));
		let enc = HdRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hd");
	}

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = Hd::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), 7);
		let enc = Hd::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hd\0\0\0\0\0\0\0\x07"
		);
	}
}
