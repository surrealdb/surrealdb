use crate::err::Error;
use crate::sql::id::Id;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
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

impl From<Thing> for Vec<u8> {
	fn from(val: Thing) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Thing {
	fn from(val: Vec<u8>) -> Self {
		Thing::decode(&val).unwrap()
	}
}

impl From<&Vec<u8>> for Thing {
	fn from(val: &Vec<u8>) -> Self {
		Thing::decode(val).unwrap()
	}
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
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		crate::sql::serde::beg_internal_serialization();
		let v = serialize(self);
		crate::sql::serde::end_internal_serialization();
		Ok(v?)
	}
	pub fn decode(v: &[u8]) -> Result<Thing, Error> {
		Ok(deserialize(v)?)
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
}
