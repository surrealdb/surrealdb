use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Ev {
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
	pub ev: String,
}

pub fn new(ns: &str, db: &str, tb: &str, ev: &str) -> Ev {
	Ev::new(ns.to_string(), db.to_string(), tb.to_string(), ev.to_string())
}

pub fn prefix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::table::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x65, 0x76, 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::table::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x65, 0x76, 0xff]);
	k
}

impl Ev {
	pub fn new(ns: String, db: String, tb: String, ev: String) -> Ev {
		Ev {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0x21, // !
			_e: 0x65, // e
			_f: 0x76, // v
			ev,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ev::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Ev::encode(&val).unwrap();
		let dec = Ev::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
