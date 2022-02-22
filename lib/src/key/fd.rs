use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Fd {
	kv: String,
	_a: String,
	ns: String,
	_b: String,
	db: String,
	_c: String,
	tb: String,
	_d: String,
	fd: String,
}

impl Into<Vec<u8>> for Fd {
	fn into(self) -> Vec<u8> {
		self.encode().unwrap()
	}
}

impl From<Vec<u8>> for Fd {
	fn from(val: Vec<u8>) -> Self {
		Fd::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, tb: &str, fd: &str) -> Fd {
	Fd::new(ns.to_string(), db.to_string(), tb.to_string(), fd.to_string())
}

impl Fd {
	pub fn new(ns: String, db: String, tb: String, fd: String) -> Fd {
		Fd {
			kv: BASE.to_owned(),
			_a: String::from("*"),
			ns,
			_b: String::from("*"),
			db,
			_c: String::from("*"),
			tb,
			_d: String::from("!fd"),
			fd,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Fd, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Fd::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Fd::encode(&val).unwrap();
		let dec = Fd::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
