use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Db {
	__: char,
	_a: char,
	pub ns: String,
	_b: char,
	_c: char,
	_d: char,
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
			__: '/',
			_a: '*',
			ns,
			_b: '!',
			_c: 'd',
			_d: 'b',
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
