//! Stores a record document
use crate::sql::id::Id;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Thing {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	pub tb: u32,
	_d: u8,
	pub id: Id,
}

pub fn new(ns: u32, db: u32, tb: u32, id: &Id) -> Thing {
	Thing::new(ns, db, tb, id.to_owned())
}

pub fn prefix(ns: u32, db: u32, tb: u32) -> Vec<u8> {
	let mut k = crate::key::table::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[b'*', 0x00]);
	k
}

pub fn suffix(ns: u32, db: u32, tb: u32) -> Vec<u8> {
	let mut k = crate::key::table::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[b'*', 0xff]);
	k
}

impl Thing {
	pub fn new(ns: u32, db: u32, tb: u32, id: Id) -> Self {
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
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Thing::new(
			1,
			2,
			3,
			"testid".into(),
		);
		let enc = Thing::encode(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*\x00\x00\x00\x03*\0\0\0\x01testid\0");

		let dec = Thing::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
	#[test]
	fn key_complex() {
		use super::*;
		//
		let id1 = "['test']";
		let (_, id1) = crate::sql::id::id(id1).expect("Failed to parse the ID");
		let val = Thing::new(1, 2, 3, id1);
		let enc = Thing::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*\x00\x00\x00\x03*\0\0\0\x02\0\0\0\x04test\0\x01"
		);

		let dec = Thing::decode(&enc).unwrap();
		assert_eq!(val, dec);
		println!("---");
		//
		let id2 = "['f8e238f2-e734-47b8-9a16-476b291bd78a']";
		let (_, id2) = crate::sql::id::id(id2).expect("Failed to parse the ID");
		let val = Thing::new(1, 2, 3, id2);
		let enc = Thing::encode(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*\x00\x00\x00\x03*\0\0\0\x02\0\0\0\x07\xf8\xe2\x38\xf2\xe7\x34\x47\xb8\x9a\x16\x47\x6b\x29\x1b\xd7\x8a\x01");

		let dec = Thing::decode(&enc).unwrap();
		assert_eq!(val, dec);
		println!("---");
	}
}
