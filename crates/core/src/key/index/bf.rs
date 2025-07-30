//! Stores Term/Doc frequency
use crate::idx::docids::DocId;
use crate::idx::ft::TermFrequency;
use crate::idx::ft::search::terms::TermId;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::KVKey;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Bf<'a> {
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
	pub term_id: TermId,
	pub doc_id: DocId,
}

impl KVKey for Bf<'_> {
	type ValueType = TermFrequency;
}

impl Categorise for Bf<'_> {
	fn categorise(&self) -> Category {
		Category::IndexTermDocFrequency
	}
}

impl<'a> Bf<'a> {
	pub fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		term_id: TermId,
		doc_id: DocId,
	) -> Self {
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
			_g: b'f',
			term_id,
			doc_id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Bf::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			7,
			13
		);
		let enc = Bf::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0+testix\0!bf\0\0\0\0\0\0\0\x07\0\0\0\0\0\0\0\x0d"
		);
	}
}
