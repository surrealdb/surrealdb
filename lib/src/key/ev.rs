use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Ev {
	kv: String,
	_a: String,
	ns: String,
	_b: String,
	db: String,
	_c: String,
	tb: String,
	_d: String,
	ev: String,
}

impl From<Ev> for Vec<u8> {
	fn from(val: Ev) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Ev {
	fn from(val: Vec<u8>) -> Self {
		Ev::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, tb: &str, ev: &str) -> Ev {
	Ev::new(ns.to_string(), db.to_string(), tb.to_string(), ev.to_string())
}

impl Ev {
	pub fn new(ns: String, db: String, tb: String, ev: String) -> Ev {
		Ev {
			kv: BASE.to_owned(),
			_a: String::from("*"),
			ns,
			_b: String::from("*"),
			db,
			_c: String::from("*"),
			tb,
			_d: String::from("!ev"),
			ev,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Ev, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ev::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Ev::encode(&val).unwrap();
		let dec = Ev::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
