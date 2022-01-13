use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Lv {
	kv: String,
	_a: String,
	ns: String,
	_b: String,
	db: String,
	_c: String,
	tb: String,
	_d: String,
	lv: String,
}

pub fn new(ns: &str, db: &str, tb: &str, lv: &str) -> Lv {
	Lv::new(ns.to_string(), db.to_string(), tb.to_string(), lv.to_string())
}

impl Lv {
	pub fn new(ns: String, db: String, tb: String, lv: String) -> Lv {
		Lv {
			kv: BASE.to_owned(),
			_a: String::from("*"),
			ns,
			_b: String::from("*"),
			db,
			_c: String::from("*"),
			tb,
			_d: String::from("!lv"),
			lv,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Lv, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Lv::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Lv::encode(&val).unwrap();
		let dec = Lv::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
