//! Stores database versionstamps
use derive::Key;
use serde::{Deserialize, Serialize};

// Vs stands for Database Versionstamp
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Vs {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	_d: u8,
	_e: u8,
}

#[allow(unused)]
pub fn new(ns: u32, db: u32) -> Vs {
	Vs::new(ns, db)
}

impl Vs {
	pub fn new(ns: u32, db: u32) -> Self {
		Vs {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'v',
			_e: b's',
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Vs::new(
			1,
			2,
		);
		let enc = Vs::encode(&val).unwrap();
		let dec = Vs::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
