use crate::err::Error;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Db {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	_c: u8,
	_d: u8,
	pub db: String,
}

impl From<Db> for Vec<u8> {
	fn from(val: Db) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Db {
	fn from(val: Vec<u8>) -> Self {
		Db::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str) -> Db {
	Db::new(ns.to_string(), db.to_string())
}

impl Db {
	pub fn new(ns: String, db: String) -> Db {
		Db {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x21, // !
			_c: 0x64, // d
			_d: 0x02, // b
			db,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Db, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Db::new(
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Db::encode(&val).unwrap();
		let dec = Db::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
