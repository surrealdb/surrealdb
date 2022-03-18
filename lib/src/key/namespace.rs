use crate::err::Error;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Namespace {
	__: u8,
	_a: u8,
	pub ns: String,
}

impl From<Namespace> for Vec<u8> {
	fn from(val: Namespace) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Namespace {
	fn from(val: Vec<u8>) -> Self {
		Namespace::decode(&val).unwrap()
	}
}

pub fn new(ns: &str) -> Namespace {
	Namespace::new(ns.to_string())
}

pub fn prefix() -> Vec<u8> {
	let mut k = super::kv::new().encode().unwrap();
	k.extend_from_slice(&[0x2a, 0x00]);
	k
}

pub fn suffix() -> Vec<u8> {
	let mut k = super::kv::new().encode().unwrap();
	k.extend_from_slice(&[0x2a, 0xff]);
	k
}

impl Namespace {
	pub fn new(ns: String) -> Namespace {
		Namespace {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Namespace, Error> {
		Ok(deserialize::<Namespace>(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Namespace::new(
			"test".to_string(),
		);
		let enc = Namespace::encode(&val).unwrap();
		let dec = Namespace::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
