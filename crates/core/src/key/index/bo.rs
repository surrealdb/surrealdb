//! Stores the offsets
use crate::idx::docids::DocId;
use crate::idx::ft::offset::OffsetRecords;
use crate::idx::ft::search::terms::TermId;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::KVKey;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Bo<'a> {
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
	pub term_id: TermId,
}

impl KVKey for Bo<'_> {
	type ValueType = OffsetRecords;
}

impl Categorise for Bo<'_> {
	fn categorise(&self) -> Category {
		Category::IndexOffset
	}
}

impl<'a> Bo<'a> {
	pub fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		doc_id: DocId,
		term_id: TermId,
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
			_g: b'o',
			doc_id,
			term_id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Bo::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			1,2
		);
		let enc = Bo::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0+testix\0!bo\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x02"
		);
	}
}
