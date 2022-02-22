use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Dt {
	kv: String,
	_a: String,
	ns: String,
	_b: String,
	db: String,
	_c: String,
	tk: String,
}

impl Into<Vec<u8>> for Dt {
	fn into(self) -> Vec<u8> {
		self.encode().unwrap()
	}
}

impl From<Vec<u8>> for Dt {
	fn from(val: Vec<u8>) -> Self {
		Dt::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, tb: &str) -> Dt {
	Dt::new(ns.to_string(), db.to_string(), tb.to_string())
}

impl Dt {
	pub fn new(ns: String, db: String, tk: String) -> Dt {
		Dt {
			kv: BASE.to_owned(),
			_a: String::from("*"),
			ns,
			_b: String::from("*"),
			db,
			_c: String::from("!tk"),
			tk,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Dt, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Dt::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Dt::encode(&val).unwrap();
		let dec = Dt::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
