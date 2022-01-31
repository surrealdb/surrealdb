use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Kv {
	kv: String,
}

impl Into<Vec<u8>> for Kv {
	fn into(self) -> Vec<u8> {
		self.encode().unwrap()
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
			kv: BASE.to_owned(),
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Kv, Error> {
		Ok(deserialize(v)?)
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
