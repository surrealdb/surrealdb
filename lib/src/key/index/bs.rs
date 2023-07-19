//! Stores FullText index states
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Bs<'a> {
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
	pub ix: &'a str,
}

impl<'a> Bs<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str) -> Self {
		Bs {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'!',
			_e: b'b',
			_f: b's',
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
		let val = Bs::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
		);
		let enc = Bs::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0!bstestix\0");

		let dec = Bs::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
