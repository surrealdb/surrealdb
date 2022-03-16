use crate::err::Error;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Sc {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	_d: u8,
	_e: u8,
	pub sc: String,
}

impl From<Sc> for Vec<u8> {
	fn from(val: Sc) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Sc {
	fn from(val: Vec<u8>) -> Self {
		Sc::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, sc: &str) -> Sc {
	Sc::new(ns.to_string(), db.to_string(), sc.to_string())
}

impl Sc {
	pub fn new(ns: String, db: String, sc: String) -> Sc {
		Sc {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x21, // !
			_d: 0x73, // s
			_e: 0x63, // c
			sc,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Sc, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Sc::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Sc::encode(&val).unwrap();
		let dec = Sc::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
