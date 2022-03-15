use crate::err::Error;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Tb {
	__: char,
	_a: char,
	pub ns: String,
	_b: char,
	pub db: String,
	_c: char,
	_d: char,
	_e: char,
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
			__: '/',
			_a: '*',
			ns,
			_b: '*',
			db,
			_c: '!',
			_d: 't',
			_e: 'b',
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
