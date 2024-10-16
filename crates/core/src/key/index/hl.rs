//! Store and chunked layers of an HNSW index
use derive::Key;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Hl<'a> {
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
	pub layer: u16,
	pub chunk: u32,
}

impl<'a> Hl<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, layer: u16, chunk: u32) -> Self {
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
			_f: b'h',
			_g: b'l',
			layer,
			chunk,
		}
	}
}

#[cfg(test)]
mod tests {

	#[test]
	fn key() {
		use super::*;
		let val = Hl::new("testns", "testdb", "testtb", "testix", 7, 8);
		let enc = Hl::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0+testix\0!hl\0\x07\0\0\0\x08",
			"{}",
			String::from_utf8_lossy(&enc)
		);

		let dec = Hl::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
