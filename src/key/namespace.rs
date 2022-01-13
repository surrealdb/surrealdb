use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Namespace {
	kv: String,
	_a: String,
	ns: String,
}

pub fn new(ns: &str) -> Namespace {
	Namespace::new(ns.to_string())
}

impl Namespace {
	pub fn new(ns: String) -> Namespace {
		Namespace {
			kv: BASE.to_owned(),
			_a: String::from("*"),
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
