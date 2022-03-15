use crate::err::Error;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Ft {
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
	pub ft: String,
}

impl From<Ft> for Vec<u8> {
	fn from(val: Ft) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Ft {
	fn from(val: Vec<u8>) -> Self {
		Ft::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, tb: &str, ft: &str) -> Ft {
	Ft::new(ns.to_string(), db.to_string(), tb.to_string(), ft.to_string())
}

impl Ft {
	pub fn new(ns: String, db: String, tb: String, ft: String) -> Ft {
		Ft {
			__: '/',
			_a: '*',
			ns,
			_b: '*',
			db,
			_c: '*',
			tb,
			_d: '!',
			_e: 'f',
			_f: 't',
			ft,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Ft, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ft::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Ft::encode(&val).unwrap();
		let dec = Ft::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
