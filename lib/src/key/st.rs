use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct St {
	kv: String,
	_a: String,
	ns: String,
	_b: String,
	db: String,
	_c: String,
	sc: String,
	_d: String,
	tk: String,
}

impl From<St> for Vec<u8> {
	fn from(val: St) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for St {
	fn from(val: Vec<u8>) -> Self {
		St::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, sc: &str, tk: &str) -> St {
	St::new(ns.to_string(), db.to_string(), sc.to_string(), tk.to_string())
}

impl St {
	pub fn new(ns: String, db: String, sc: String, tk: String) -> St {
		St {
			kv: BASE.to_owned(),
			_a: String::from("*"),
			ns,
			_b: String::from("*"),
			db,
			_c: String::from("!st"),
			sc,
			_d: String::from("!tk"),
			tk,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<St, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = St::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = St::encode(&val).unwrap();
		let dec = St::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
