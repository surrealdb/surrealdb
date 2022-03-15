use crate::err::Error;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Sc {
	__: char,
	_a: char,
	pub ns: String,
	_b: char,
	pub db: String,
	_c: char,
	_d: char,
	_e: char,
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
			__: '/',
			_a: '*',
			ns,
			_b: '*',
			db,
			_c: '!',
			_d: 's',
			_e: 'c',
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
