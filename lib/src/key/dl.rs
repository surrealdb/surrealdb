use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Dl {
	__: char,
	_a: char,
	pub ns: String,
	_b: char,
	pub db: String,
	_c: char,
	_d: char,
	_e: char,
	pub dl: String,
}

impl From<Dl> for Vec<u8> {
	fn from(val: Dl) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Dl {
	fn from(val: Vec<u8>) -> Self {
		Dl::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, dl: &str) -> Dl {
	Dl::new(ns.to_string(), db.to_string(), dl.to_string())
}

impl Dl {
	pub fn new(ns: String, db: String, dl: String) -> Dl {
		Dl {
			__: '/',
			_a: '*',
			ns,
			_b: '*',
			db,
			_c: '!',
			_d: 'd',
			_e: 'l',
			dl,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Dl, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Dl::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Dl::encode(&val).unwrap();
		let dec = Dl::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
