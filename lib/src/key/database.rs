use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Database {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
}

pub fn new(ns: &str, db: &str) -> Database {
	Database::new(ns.to_string(), db.to_string())
}

impl Database {
	pub fn new(ns: String, db: String) -> Database {
		Database {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
		}
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
