//! Store appended records for concurrent index building
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::kvs::KVKey;
use crate::kvs::index::Appending;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Ia<'a> {
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

impl KVKey for Ia<'_> {
	type ValueType = Appending;
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
	use super::*;

	#[test]
	fn key() {
		let val = Ia::new("testns", "testdb", "testtb", "testix", 1);
		let enc = Ia::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0+testix\0!ia\x00\x00\x00\x01",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
