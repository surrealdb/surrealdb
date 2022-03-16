use crate::err::Error;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Dt {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	_d: u8,
	_e: u8,
	pub tk: String,
}

impl From<Dt> for Vec<u8> {
	fn from(val: Dt) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Dt {
	fn from(val: Vec<u8>) -> Self {
		Dt::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, tb: &str) -> Dt {
	Dt::new(ns.to_string(), db.to_string(), tb.to_string())
}

impl Dt {
	pub fn new(ns: String, db: String, tk: String) -> Dt {
		Dt {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x21, // !
			_d: 0x64, // d
			_e: 0x74, // t
			tk,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Dt, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Dt::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Dt::encode(&val).unwrap();
		let dec = Dt::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
