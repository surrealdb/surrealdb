//! Stores a record document
use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
use crate::sql::id::Id;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Thing<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: Id,
}

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str, id: &Id) -> Thing<'a> {
	Thing::new(ns, db, tb, id.to_owned())
}

pub fn prefix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = crate::key::table::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[b'*', 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = crate::key::table::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[b'*', 0xff]);
	k
}

impl KeyRequirements for Thing<'_> {
	fn key_category(&self) -> KeyCategory {
		KeyCategory::Thing
	}
}

impl<'a> Thing<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, id: Id) -> Self {
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
}

#[cfg(test)]
mod tests {
	use crate::syn;

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Thing::new(
			"testns",
			"testdb",
			"testtb",
			"testid".into(),
		);
		let enc = Thing::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0*\0\0\0\x01testid\0");

		let dec = Thing::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
	#[test]
	fn key_complex() {
		use super::*;
		//
		let id1 = "foo:['test']";
		let thing = syn::thing(id1).expect("Failed to parse the ID");
		let id1 = thing.id;
		let val = Thing::new("testns", "testdb", "testtb", id1);
		let enc = Thing::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0*\0\0\0\x02\0\0\0\x04test\0\x01");

		let dec = Thing::decode(&enc).unwrap();
		assert_eq!(val, dec);
		println!("---");
		let id2 = "foo:[u'f8e238f2-e734-47b8-9a16-476b291bd78a']";
		let thing = syn::thing(id2).expect("Failed to parse the ID");
		let id2 = thing.id;
		let val = Thing::new("testns", "testdb", "testtb", id2);
		let enc = Thing::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0*\0\0\0\x02\0\0\0\x07\0\0\0\0\0\0\0\x10\xf8\xe2\x38\xf2\xe7\x34\x47\xb8\x9a\x16\x47\x6b\x29\x1b\xd7\x8a\x01");

		let dec = Thing::decode(&enc).unwrap();
		assert_eq!(val, dec);
		println!("---");
	}
}
