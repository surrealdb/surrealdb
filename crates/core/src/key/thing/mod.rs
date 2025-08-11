//! Stores a record document
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use crate::val::{RecordIdKey, Value};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Thing<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: RecordIdKey,
}

impl KVKey for Thing<'_> {
	type ValueType = Value;
}

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str, id: &RecordIdKey) -> Thing<'a> {
	Thing::new(ns, db, tb, id.to_owned())
}

pub fn prefix(ns: &str, db: &str, tb: &str) -> Result<Vec<u8>> {
	let mut k = crate::key::table::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"*\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str, tb: &str) -> Result<Vec<u8>> {
	let mut k = crate::key::table::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"*\xff");
	Ok(k)
}

impl Categorise for Thing<'_> {
	fn categorise(&self) -> Category {
		Category::Thing
	}
}

impl<'a> Thing<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, id: RecordIdKey) -> Self {
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

	pub fn decode_key(k: &[u8]) -> Result<Thing<'_>> {
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
		let val = Thing::new(
			"testns",
			"testdb",
			"testtb",
			RecordIdKey::String("testid".to_owned()),
		);
		let enc = Thing::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0*\0\0\0\x01testid\0");
	}
	#[test]
	fn key_complex() {
		//
		let id1 = "foo:['test']";
		let thing = syn::thing(id1).expect("Failed to parse the ID");
		let id1 = thing.key;
		let val = Thing::new("testns", "testdb", "testtb", id1);
		let enc = Thing::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0*\0\0\0\x03\0\0\0\x04test\0\x01");

		println!("---");
		let id2 = "foo:[u'f8e238f2-e734-47b8-9a16-476b291bd78a']";
		let thing = syn::thing(id2).expect("Failed to parse the ID");
		let id2 = thing.key;
		let val = Thing::new("testns", "testdb", "testtb", id2);
		let enc = Thing::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0*\0\0\0\x03\0\0\0\x07\0\0\0\0\0\0\0\x10\xf8\xe2\x38\xf2\xe7\x34\x47\xb8\x9a\x16\x47\x6b\x29\x1b\xd7\x8a\x01");

		println!("---");
	}
}
