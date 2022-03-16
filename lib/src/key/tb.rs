use crate::err::Error;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Tb {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	_d: u8,
	_e: u8,
	pub tb: String,
}

impl From<Tb> for Vec<u8> {
	fn from(val: Tb) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Tb {
	fn from(val: Vec<u8>) -> Self {
		Tb::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, tb: &str) -> Tb {
	Tb::new(ns.to_string(), db.to_string(), tb.to_string())
}

impl Tb {
	pub fn new(ns: String, db: String, tb: String) -> Tb {
		Tb {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x21, // !
			_d: 0x74, // t
			_e: 0x62, // b
			tb,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Tb, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Tb::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Tb::encode(&val).unwrap();
		let dec = Tb::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
