//! Stores a record document
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use crate::val::RecordIdKey;
use crate::val::record::Record;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct ThingKey<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: RecordIdKey,
}

impl KVKey for ThingKey<'_> {
	type ValueType = Record;
}

pub fn new<'a>(ns: NamespaceId, db: DatabaseId, tb: &'a str, id: &RecordIdKey) -> ThingKey<'a> {
	ThingKey::new(ns, db, tb, id.to_owned())
}

pub fn prefix(ns: NamespaceId, db: DatabaseId, tb: &str) -> Result<Vec<u8>> {
	let mut k = crate::key::table::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"*\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId, tb: &str) -> Result<Vec<u8>> {
	let mut k = crate::key::table::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"*\xff");
	Ok(k)
}

impl Categorise for ThingKey<'_> {
	fn categorise(&self) -> Category {
		Category::Thing
	}
}

impl<'a> ThingKey<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, id: RecordIdKey) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'*',
			id,
		}
	}

	pub fn decode_key(k: &[u8]) -> Result<ThingKey<'_>> {
		Ok(storekey::deserialize(k)?)
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::syn;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = ThingKey::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			RecordIdKey::String("testid".to_owned()),
		);
		let enc = ThingKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*\0\0\0\x01testid\0");
	}
	#[test]
	fn key_complex() {
		//
		let id1 = "foo:['test']";
		let record_id = syn::record_id(id1).expect("Failed to parse the ID");
		let id1 = record_id.key;
		let val = ThingKey::new(NamespaceId(1), DatabaseId(2), "testtb", id1);
		let enc = ThingKey::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*\0\0\0\x03\0\0\0\x04test\0\x01"
		);

		let id2 = "foo:[u'f8e238f2-e734-47b8-9a16-476b291bd78a']";
		let record_id = syn::record_id(id2).expect("Failed to parse the ID");
		let id2 = record_id.key;
		let val = ThingKey::new(NamespaceId(1), DatabaseId(2), "testtb", id2);
		let enc = ThingKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*\0\0\0\x03\0\0\0\x07\0\0\0\0\0\0\0\x10\xf8\xe2\x38\xf2\xe7\x34\x47\xb8\x9a\x16\x47\x6b\x29\x1b\xd7\x8a\x01");
	}
}
