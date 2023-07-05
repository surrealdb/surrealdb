use crate::idx::ft::docids::DocId;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Bk<'a> {
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
	pub doc_id: DocId,
}

impl<'a> Bk<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, doc_id: DocId) -> Self {
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
			_f: b'k',
			ix,
			_g: b'*',
			doc_id,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Bk::new(
			"test",
			"test",
			"test",
			"test",
			7
		);
		let enc = Bk::encode(&val).unwrap();
		let dec = Bk::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
