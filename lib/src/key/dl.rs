use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Dl {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	_d: u8,
	_e: u8,
	pub dl: String,
}

pub fn new(ns: &str, db: &str, dl: &str) -> Dl {
	Dl::new(ns.to_string(), db.to_string(), dl.to_string())
}

pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x64, 0x6c, 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x64, 0x6c, 0xff]);
	k
}

impl Dl {
	pub fn new(ns: String, db: String, dl: String) -> Dl {
		Dl {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x21, // !
			_d: 0x64, // d
			_e: 0x6c, // l
			dl,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Dl::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Dl::encode(&val).unwrap();
		let dec = Dl::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
