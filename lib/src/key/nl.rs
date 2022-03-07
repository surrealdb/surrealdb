use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Nl {
	__: char,
	_a: char,
	pub ns: String,
	_b: char,
	_c: char,
	_d: char,
	pub us: String,
}

impl From<Nl> for Vec<u8> {
	fn from(val: Nl) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Nl {
	fn from(val: Vec<u8>) -> Self {
		Nl::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, us: &str) -> Nl {
	Nl::new(ns.to_string(), us.to_string())
}

impl Nl {
	pub fn new(ns: String, us: String) -> Nl {
		Nl {
			__: '/',
			_a: '*',
			ns,
			_b: '!',
			_c: 'n',
			_d: 'l',
			us,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Nl, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Nl::new(
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Nl::encode(&val).unwrap();
		let dec = Nl::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
