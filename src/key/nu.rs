use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Nu {
	kv: String,
	_a: String,
	ns: String,
	_b: String,
	us: String,
}

impl Into<Vec<u8>> for Nu {
	fn into(self) -> Vec<u8> {
		self.encode().unwrap()
	}
}

impl From<Vec<u8>> for Nu {
	fn from(val: Vec<u8>) -> Self {
		Nu::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, us: &str) -> Nu {
	Nu::new(ns.to_string(), us.to_string())
}

impl Nu {
	pub fn new(ns: String, us: String) -> Nu {
		Nu {
			kv: BASE.to_owned(),
			_a: String::from("*"),
			ns,
			_b: String::from("!us"),
			us,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Nu, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Nu::new(
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Nu::encode(&val).unwrap();
		let dec = Nu::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
