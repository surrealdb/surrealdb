use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Lv {
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
	pub lv: String,
}

impl From<Lv> for Vec<u8> {
	fn from(val: Lv) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Lv {
	fn from(val: Vec<u8>) -> Self {
		Lv::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, tb: &str, lv: &str) -> Lv {
	Lv::new(ns.to_string(), db.to_string(), tb.to_string(), lv.to_string())
}

impl Lv {
	pub fn new(ns: String, db: String, tb: String, lv: String) -> Lv {
		Lv {
			__: '/',
			_a: '*',
			ns,
			_b: '*',
			db,
			_c: '*',
			tb,
			_d: '!',
			_e: 'l',
			_f: 'v',
			lv,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Lv, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Lv::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Lv::encode(&val).unwrap();
		let dec = Lv::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
