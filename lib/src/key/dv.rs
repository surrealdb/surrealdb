use derive::Key;
use serde::{Deserialize, Serialize};

// Dv stands for Database Versionstamp
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Dv {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_d: u8,
	_e: u8,
	_f: u8,
}

#[allow(unused)]
pub fn new(ns: &str, db: &str) -> Dv {
	Dv::new(ns.to_string(), db.to_string())
}

impl Dv {
	pub fn new(ns: String, db: String) -> Dv {
		Dv {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_d: b'!',
			_e: b't',
			_f: b't',
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Dv::new(
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Dv::encode(&val).unwrap();
		let dec = Dv::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
