//! Stores doc keys for doc_ids
use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::seqdocids::DocId;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::val::{RecordIdKey, TableName};

/// Id inverted. DocId -> Id
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Ii<'a> {
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
	pub id: DocId,
}

impl_kv_key_storekey!(Ii<'_> => RecordIdKey);

impl Categorise for Ii<'_> {
	fn categorise(&self) -> Category {
		Category::IndexDocKeys
	}
}

impl<'a> Ii<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, ix: IndexId, id: DocId) -> Self {
		Ii {
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
		let tb = TableName::from("testtb");
		let val = Ii::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), 1);
		let enc = Ii::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!ii\0\0\0\0\0\0\0\x01"
		);
	}
}
