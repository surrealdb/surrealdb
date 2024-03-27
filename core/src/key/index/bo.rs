//! Stores the offsets
use crate::idx::docids::DocId;
use crate::idx::ft::terms::TermId;
use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Bo<'a> {
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

impl KeyRequirements for Bo<'_> {
	fn key_category(&self) -> KeyCategory {
		KeyCategory::IndexOffset
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
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Bo::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			1,2
		);
		let enc = Bo::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0+testix\0!bo\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x02"
		);

		let dec = Bo::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
