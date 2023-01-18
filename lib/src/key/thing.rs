use crate::sql::id::Id;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Thing {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	pub tb: String,
	_d: u8,
	pub id: Id,
}

pub fn new(ns: &str, db: &str, tb: &str, id: &Id) -> Thing {
	Thing::new(ns.to_string(), db.to_string(), tb.to_string(), id.to_owned())
}

pub fn prefix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::table::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[0x2a, 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::table::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[0x2a, 0xff]);
	k
}

impl Thing {
	pub fn new(ns: String, db: String, tb: String, id: Id) -> Thing {
		Thing {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0x2a, // *
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
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".into(),
		);
		let enc = Thing::encode(&val).unwrap();
		let dec = Thing::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
	#[test]
	fn key_complex() {
		use super::*;
		//
		let id1 = "['test']";
		let (_, id1) = crate::sql::id::id(id1).expect("Failed to parse the ID");
		let val = Thing::new("test".to_string(), "test".to_string(), "test".to_string(), id1);
		let enc = Thing::encode(&val).unwrap();
		let dec = Thing::decode(&enc).unwrap();
		assert_eq!(val, dec);
		println!("---");
		//
		let id2 = "['f8e238f2-e734-47b8-9a16-476b291bd78a']";
		let (_, id2) = crate::sql::id::id(id2).expect("Failed to parse the ID");
		let val = Thing::new("test".to_string(), "test".to_string(), "test".to_string(), id2);
		let enc = Thing::encode(&val).unwrap();
		let dec = Thing::decode(&enc).unwrap();
		assert_eq!(val, dec);
		println!("---");
	}
}
