use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Fd {
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
	pub fd: String,
}

impl From<Fd> for Vec<u8> {
	fn from(val: Fd) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Fd {
	fn from(val: Vec<u8>) -> Self {
		Fd::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, tb: &str, fd: &str) -> Fd {
	Fd::new(ns.to_string(), db.to_string(), tb.to_string(), fd.to_string())
}

impl Fd {
	pub fn new(ns: String, db: String, tb: String, fd: String) -> Fd {
		Fd {
			__: '/',
			_a: '*',
			ns,
			_b: '*',
			db,
			_c: '*',
			tb,
			_d: '!',
			_e: 'f',
			_f: 'd',
			fd,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Fd, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Fd::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Fd::encode(&val).unwrap();
		let dec = Fd::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
