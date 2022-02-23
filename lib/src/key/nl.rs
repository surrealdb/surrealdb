use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Nl {
	kv: String,
	_a: String,
	ns: String,
	_b: String,
	us: String,
}

impl Into<Vec<u8>> for Nl {
	fn into(self) -> Vec<u8> {
		self.encode().unwrap()
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
