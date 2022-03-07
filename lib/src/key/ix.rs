use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Ix {
	__: char,
	_a: char,
	pub ns: String,
	_b: char,
	pub db: String,
	_c: char,
	pub tb: String,
	_d: char,
	_e: char,
	_f: char,
	pub ix: String,
}

impl From<Ix> for Vec<u8> {
	fn from(val: Ix) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Ix {
	fn from(val: Vec<u8>) -> Self {
		Ix::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, tb: &str, ix: &str) -> Ix {
	Ix::new(ns.to_string(), db.to_string(), tb.to_string(), ix.to_string())
}

impl Ix {
	pub fn new(ns: String, db: String, tb: String, ix: String) -> Ix {
		Ix {
			__: '/',
			_a: '*',
			ns,
			_b: '*',
			db,
			_c: '*',
			tb,
			_d: '!',
			_e: 'i',
			_f: 'x',
			ix,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Ix, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ix::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Ix::encode(&val).unwrap();
		let dec = Ix::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
