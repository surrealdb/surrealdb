use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Scope {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	pub sc: String,
}

pub fn new(ns: &str, db: &str, sc: &str) -> Scope {
	Scope::new(ns.to_string(), db.to_string(), sc.to_string())
}

impl Scope {
	pub fn new(ns: String, db: String, sc: String) -> Scope {
		Scope {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0xb1, // ±
			sc,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Scope::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Scope::encode(&val).unwrap();
		let dec = Scope::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
