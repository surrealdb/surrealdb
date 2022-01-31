use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Ns {
	kv: String,
	_a: String,
	ns: String,
}

impl Into<Vec<u8>> for Ns {
	fn into(self) -> Vec<u8> {
		self.encode().unwrap()
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
			kv: BASE.to_owned(),
			_a: String::from("!ns"),
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
