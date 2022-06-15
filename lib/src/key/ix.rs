use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Ix {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	pub tb: String,
	_d: u8,
	_e: u8,
	_f: u8,
	pub ix: String,
}

pub fn new(ns: &str, db: &str, tb: &str, ix: &str) -> Ix {
	Ix::new(ns.to_string(), db.to_string(), tb.to_string(), ix.to_string())
}

pub fn prefix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::table::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x69, 0x78, 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::table::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x69, 0x78, 0xff]);
	k
}

impl Ix {
	pub fn new(ns: String, db: String, tb: String, ix: String) -> Ix {
		Ix {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0x21, // !
			_e: 0x69, // i
			_f: 0x78, // x
			ix,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ix::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Ix::encode(&val).unwrap();
		let dec = Ix::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
