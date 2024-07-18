//! Stores the term list for doc_ids
use crate::idx::docids::DocId;
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Bk<'a> {
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
	pub doc_id: DocId,
}

impl Categorise for Bk<'_> {
	fn categorise(&self) -> Category {
		Category::IndexTermList
	}
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
			_d: b'+',
			ix,
			_e: b'!',
			_f: b'b',
			_g: b'k',
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
			"testns",
			"testdb",
			"testtb",
			"testix",
			7
		);
		let enc = Bk::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!bk\0\0\0\0\0\0\0\x07");

		let dec = Bk::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
