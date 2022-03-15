use crate::err::Error;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Database {
	__: char,
	_a: char,
	pub ns: String,
	_b: char,
	pub db: String,
}

impl From<Database> for Vec<u8> {
	fn from(val: Database) -> Vec<u8> {
		val.encode().unwrap()
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
			__: '/',
			_a: '*',
			ns,
			_b: '*',
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
