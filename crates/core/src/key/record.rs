//! Stores a record document
use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId, Record};
use crate::key::category::{Categorise, Category};
use crate::kvs::{KVKey, impl_kv_key_storekey};
use crate::val::{RecordIdKey, TableName};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct RecordKey<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	pub id: RecordIdKey,
}

impl_kv_key_storekey!(RecordKey<'_> => Record);

pub fn new<'a>(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &'a TableName,
	id: &RecordIdKey,
) -> RecordKey<'a> {
	RecordKey::new(ns, db, tb, id.to_owned())
}

pub fn prefix(ns: NamespaceId, db: DatabaseId, tb: &TableName) -> Result<Vec<u8>> {
	let mut k = crate::key::table::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"*\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId, tb: &TableName) -> Result<Vec<u8>> {
	let mut k = crate::key::table::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"*\xff");
	Ok(k)
}

impl Categorise for RecordKey<'_> {
	fn categorise(&self) -> Category {
		Category::Record
	}
}

impl<'a> RecordKey<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, id: RecordIdKey) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'*',
			id,
		}
	}

	pub fn decode_key(k: &[u8]) -> Result<RecordKey<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::syn;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = RecordKey::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			RecordIdKey::String("testid".to_owned()),
		);
		let enc = RecordKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*\x03testid\0");
	}
	#[test]
	fn key_complex() {
		//
		let id1 = "foo:['test']";
		let record_id = syn::record_id(id1).expect("Failed to parse the ID");
		let id1 = record_id.key.into();
		let val = RecordKey::new(NamespaceId(1), DatabaseId(2), "testtb", id1);
		let enc = RecordKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*\x05\x06test\0\0");

		let id2 = "foo:[u'f8e238f2-e734-47b8-9a16-476b291bd78a']";
		let record_id = syn::record_id(id2).expect("Failed to parse the ID");
		let id2 = record_id.key.into();
		let val = RecordKey::new(NamespaceId(1), DatabaseId(2), "testtb", id2);
		let enc = RecordKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*\x05\x09\xf8\xe2\x38\xf2\xe7\x34\x47\xb8\x9a\x16\x47\x6b\x29\x1b\xd7\x8a\x00");
	}
}
