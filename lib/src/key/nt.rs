use crate::err::Error;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Nt {
	__: char,
	_a: char,
	pub ns: String,
	_b: char,
	_c: char,
	_d: char,
	pub tk: String,
}

impl From<Nt> for Vec<u8> {
	fn from(val: Nt) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Nt {
	fn from(val: Vec<u8>) -> Self {
		Nt::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, tk: &str) -> Nt {
	Nt::new(ns.to_string(), tk.to_string())
}

impl Nt {
	pub fn new(ns: String, tk: String) -> Nt {
		Nt {
			__: '/',
			_a: '*',
			ns,
			_b: '!',
			_c: 'n',
			_d: 't',
			tk,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Nt, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Nt::new(
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Nt::encode(&val).unwrap();
		let dec = Nt::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
