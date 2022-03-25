use crate::err::Error;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Database {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
}

impl From<Database> for Vec<u8> {
	fn from(val: Database) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Database {
	fn from(val: Vec<u8>) -> Self {
		Database::decode(&val).unwrap()
	}
}

impl From<&Vec<u8>> for Database {
	fn from(val: &Vec<u8>) -> Self {
		Database::decode(val).unwrap()
	}
}

pub fn new(ns: &str, db: &str) -> Database {
	Database::new(ns.to_string(), db.to_string())
}

pub fn prefix(ns: &str) -> Vec<u8> {
	let mut k = super::namespace::new(ns).encode().unwrap();
	k.extend_from_slice(&[0x2a, 0x00]);
	k
}

pub fn suffix(ns: &str) -> Vec<u8> {
	let mut k = super::namespace::new(ns).encode().unwrap();
	k.extend_from_slice(&[0x2a, 0xff]);
	k
}

impl Database {
	pub fn new(ns: String, db: String) -> Database {
		Database {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Database, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Database::new(
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Database::encode(&val).unwrap();
		let dec = Database::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
