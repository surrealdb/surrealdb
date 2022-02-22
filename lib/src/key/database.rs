use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Database {
	kv: String,
	_a: String,
	ns: String,
	_b: String,
	db: String,
}

impl Into<Vec<u8>> for Database {
	fn into(self) -> Vec<u8> {
		self.encode().unwrap()
	}
}

impl From<Vec<u8>> for Database {
	fn from(val: Vec<u8>) -> Self {
		Database::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str) -> Database {
	Database::new(ns.to_string(), db.to_string())
}

impl Database {
	pub fn new(ns: String, db: String) -> Database {
		Database {
			kv: BASE.to_owned(),
			_a: String::from("*"),
			ns,
			_b: String::from("*"),
			db,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Database, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Database::new(
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Database::encode(&val).unwrap();
		let dec = Database::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
