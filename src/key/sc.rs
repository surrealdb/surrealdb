use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Sc {
	kv: String,
	_a: String,
	ns: String,
	_b: String,
	db: String,
	_c: String,
	sc: String,
}

pub fn new(ns: &str, db: &str, sc: &str) -> Sc {
	Sc::new(ns.to_string(), db.to_string(), sc.to_string())
}

impl Sc {
	pub fn new(ns: String, db: String, sc: String) -> Sc {
		Sc {
			kv: BASE.to_owned(),
			_a: String::from("*"),
			ns,
			_b: String::from("*"),
			db,
			_c: String::from("!sc"),
			sc,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Sc, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Sc::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Sc::encode(&val).unwrap();
		let dec = Sc::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
