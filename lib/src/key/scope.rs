use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Scope<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub sc: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, sc: &'a str) -> Scope<'a> {
	Scope::new(ns, db, sc)
}

impl<'a> Scope<'a> {
	pub fn new(ns: &'a str, db: &'a str, sc: &'a str) -> Self {
		Self {
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
			"test",
			"test",
			"test",
		);
		let enc = Scope::encode(&val).unwrap();
		let dec = Scope::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
