use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Fd {
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
	pub fd: String,
}

pub fn new(ns: &str, db: &str, tb: &str, fd: &str) -> Fd {
	Fd::new(ns.to_string(), db.to_string(), tb.to_string(), fd.to_string())
}

pub fn prefix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::table::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x66, 0x64, 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::table::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x66, 0x64, 0xff]);
	k
}

impl Fd {
	pub fn new(ns: String, db: String, tb: String, fd: String) -> Fd {
		Fd {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0x21, // !
			_e: 0x66, // f
			_f: 0x64, // d
			fd,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Fd::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Fd::encode(&val).unwrap();
		let dec = Fd::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
