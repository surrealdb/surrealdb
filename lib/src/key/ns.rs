use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Ns {
	__: char,
	_a: char,
	_b: char,
	_c: char,
	pub ns: String,
}

impl From<Ns> for Vec<u8> {
	fn from(val: Ns) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Ns {
	fn from(val: Vec<u8>) -> Self {
		Ns::decode(&val).unwrap()
	}
}

pub fn new(ns: &str) -> Ns {
	Ns::new(ns.to_string())
}

impl Ns {
	pub fn new(ns: String) -> Ns {
		Ns {
			__: '/',
			_a: '!',
			_b: 'n',
			_c: 's',
			ns,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Ns, Error> {
		Ok(deserialize::<Ns>(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ns::new(
			"test".to_string(),
		);
		let enc = Ns::encode(&val).unwrap();
		let dec = Ns::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
