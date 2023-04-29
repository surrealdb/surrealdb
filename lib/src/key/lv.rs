use crate::sql::uuid::Uuid;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Lv<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	_e: u8,
	_f: u8,
	pub lv: Uuid,
}

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str, lv: &Uuid) -> Lv<'a> {
	Lv::new(ns, db, tb, lv.to_owned())
}

pub fn prefix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::table::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x6c, 0x76, 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::table::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x6c, 0x76, 0xff]);
	k
}

impl<'a> Lv<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, lv: Uuid) -> Self {
		Self {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0x21, // !
			_e: 0x6c, // l
			_f: 0x76, // v
			lv,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Lv::new(
			"test",
			"test",
			"test",
			Uuid::default(),
		);
		let enc = Lv::encode(&val).unwrap();
		let dec = Lv::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
