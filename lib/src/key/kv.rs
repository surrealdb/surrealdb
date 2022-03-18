use crate::err::Error;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Kv {
	__: u8,
}

impl From<Kv> for Vec<u8> {
	fn from(val: Kv) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Kv {
	fn from(val: Vec<u8>) -> Self {
		Kv::decode(&val).unwrap()
	}
}

pub fn new() -> Kv {
	Kv::new()
}

impl Kv {
	pub fn new() -> Kv {
		Kv {
			__: 0x2f, // /
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Kv, Error> {
		Ok(deserialize::<Kv>(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Kv::new();
		let enc = Kv::encode(&val).unwrap();
		let dec = Kv::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
