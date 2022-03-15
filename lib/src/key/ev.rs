use crate::err::Error;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Ev {
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
	pub ev: String,
}

impl From<Ev> for Vec<u8> {
	fn from(val: Ev) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Ev {
	fn from(val: Vec<u8>) -> Self {
		Ev::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, tb: &str, ev: &str) -> Ev {
	Ev::new(ns.to_string(), db.to_string(), tb.to_string(), ev.to_string())
}

impl Ev {
	pub fn new(ns: String, db: String, tb: String, ev: String) -> Ev {
		Ev {
			__: '/',
			_a: '*',
			ns,
			_b: '*',
			db,
			_c: '*',
			tb,
			_d: '!',
			_e: 'e',
			_f: 'v',
			ev,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Ev, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ev::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Ev::encode(&val).unwrap();
		let dec = Ev::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
