use derive::Key;
use serde::{Deserialize, Serialize};

// Dv stands for Database Versionstamp
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Dv<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_d: u8,
	_e: u8,
	_f: u8,
}

#[allow(unused)]
pub fn new<'a>(ns: &'a str, db: &'a str) -> Dv<'a> {
	Dv::new(ns, db)
}

impl<'a> Dv<'a> {
	pub fn new(ns: &'a str, db: &'a str) -> Self {
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
			"test",
			"test",
		);
		let enc = Dv::encode(&val).unwrap();
		let dec = Dv::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
