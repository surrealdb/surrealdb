use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Dl {
	kv: String,
	_a: String,
	ns: String,
	_b: String,
	db: String,
	_c: String,
	us: String,
}

impl Into<Vec<u8>> for Dl {
	fn into(self) -> Vec<u8> {
		self.encode().unwrap()
	}
}

impl From<Vec<u8>> for Dl {
	fn from(val: Vec<u8>) -> Self {
		Dl::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, us: &str) -> Dl {
	Dl::new(ns.to_string(), db.to_string(), us.to_string())
}

impl Dl {
	pub fn new(ns: String, db: String, us: String) -> Dl {
		Dl {
			kv: BASE.to_owned(),
			_a: String::from("*"),
			ns,
			_b: String::from("*"),
			db,
			_c: String::from("!us"),
			us,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Dl, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Dl::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Dl::encode(&val).unwrap();
		let dec = Dl::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
