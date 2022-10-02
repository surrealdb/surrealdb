use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct St {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	pub sc: String,
	_d: u8,
	_e: u8,
	_f: u8,
	pub tk: String,
}

pub fn new(ns: &str, db: &str, sc: &str, tk: &str) -> St {
	St::new(ns.to_string(), db.to_string(), sc.to_string(), tk.to_string())
}

pub fn prefix(ns: &str, db: &str, sc: &str) -> Vec<u8> {
	let mut k = super::scope::new(ns, db, sc).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x73, 0x74, 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, sc: &str) -> Vec<u8> {
	let mut k = super::scope::new(ns, db, sc).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x73, 0x74, 0xff]);
	k
}

impl St {
	pub fn new(ns: String, db: String, sc: String, tk: String) -> St {
		St {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0xb1, // ±
			sc,
			_d: 0x21, // !
			_e: 0x73, // s
			_f: 0x74, // t
			tk,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = St::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = St::encode(&val).unwrap();
		let dec = St::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
