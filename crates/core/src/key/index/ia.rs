//! Store appended records for concurrent index building
use derive::Key;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Ia<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	_f: u8,
	_g: u8,
	pub i: u32,
}

impl<'a> Ia<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, i: u32) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'+',
			ix,
			_e: b'!',
			_f: b'i',
			_g: b'a',
			i,
		}
	}
}

#[cfg(test)]
mod tests {

	#[test]
	fn key() {
		use super::*;
		let val = Ia::new("testns", "testdb", "testtb", "testix", 1);
		let enc = Ia::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0+testix\0!ia\x00\x00\x00\x01",
			"{}",
			String::from_utf8_lossy(&enc)
		);

		let dec = Ia::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
