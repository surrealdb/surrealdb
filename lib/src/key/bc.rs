use crate::idx::ft::terms::TermId;
use derive::Key;
use serde::{Deserialize, Serialize};
use std::ops::Range;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Bc<'a> {
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
	_g: u8,
	pub term_id: TermId,
}

impl<'a> Bc<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, term_id: TermId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'!',
			_e: b'b',
			_f: b'c',
			ix,
			_g: b'*',
			term_id,
		}
	}

	pub fn range(ns: &str, db: &str, tb: &str, ix: &str) -> Range<Vec<u8>> {
		let mut beg = Prefix::new(ns, db, tb, ix).encode().unwrap();
		beg.extend_from_slice(&[0x00]);
		let mut end = Prefix::new(ns, db, tb, ix).encode().unwrap();
		end.extend_from_slice(&[0xff]);
		beg..end
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
struct Prefix<'a> {
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
	_g: u8,
}

impl<'a> Prefix<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'!',
			_e: b'b',
			_f: b'c',
			ix,
			_g: b'*',
		}
	}
}

#[cfg(test)]
mod tests {

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Bc::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			7
		);
		let enc = Bc::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0!bctestix\0*\0\0\0\0\0\0\0\x07");

		let dec = Bc::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
