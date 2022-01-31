use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Nt {
	kv: String,
	_a: String,
	ns: String,
	_b: String,
	tk: String,
}

impl Into<Vec<u8>> for Nt {
	fn into(self) -> Vec<u8> {
		self.encode().unwrap()
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
			kv: BASE.to_owned(),
			_a: String::from("*"),
			ns,
			_b: String::from("!tk"),
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
