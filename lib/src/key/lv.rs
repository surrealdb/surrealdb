use derive::Key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
	#[serde(with = "uuid::serde::compact")]
	pub lv: Uuid,
}

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str, lv: Uuid) -> Lv<'a> {
	Lv::new(ns, db, tb, lv)
}

pub fn prefix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::table::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[b'!', b'l', b'v', 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::table::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[b'!', b'l', b'v', 0xff]);
	k
}

impl<'a> Lv<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, lv: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'!',
			_e: b'l',
			_f: b'v',
			lv,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::key::debug;

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let live_query_id = Uuid::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
		let val = Lv::new("testns", "testdb", "testtb", live_query_id);
		let enc = Lv::encode(&val).unwrap();
		println!("{:?}", debug::sprint_key(&enc));
		assert_eq!(
			enc,
			b"/*testns\x00*testdb\x00*testtb\x00!lv\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10"
		);

		let dec = Lv::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn prefix() {
		let val = super::prefix("testns", "testdb", "testtb");
		assert_eq!(val, b"/*testns\x00*testdb\x00*testtb\x00!lv\x00")
	}

	#[test]
	fn suffix() {
		let val = super::suffix("testns", "testdb", "testtb");
		assert_eq!(val, b"/*testns\x00*testdb\x00*testtb\x00!lv\xff")
	}
}
