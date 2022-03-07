use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct St {
	__: char,
	_a: char,
	pub ns: String,
	_b: char,
	pub db: String,
	_c: char,
	_d: char,
	_e: char,
	pub sc: String,
	_f: char,
	_g: char,
	_h: char,
	pub tk: String,
}

impl From<St> for Vec<u8> {
	fn from(val: St) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for St {
	fn from(val: Vec<u8>) -> Self {
		St::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, sc: &str, tk: &str) -> St {
	St::new(ns.to_string(), db.to_string(), sc.to_string(), tk.to_string())
}

impl St {
	pub fn new(ns: String, db: String, sc: String, tk: String) -> St {
		St {
			__: '/',
			_a: '*',
			ns,
			_b: '*',
			db,
			_c: '!',
			_d: 's',
			_e: 't',
			sc,
			_f: '!',
			_g: 't',
			_h: 'k',
			tk,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<St, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = St::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = St::encode(&val).unwrap();
		let dec = St::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
