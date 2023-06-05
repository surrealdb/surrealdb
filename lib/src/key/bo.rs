use crate::idx::ft::docids::DocId;
use crate::idx::ft::terms::TermId;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Bo<'a> {
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
	pub term_id: TermId,
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
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0x21, // !
			_e: 0x62, // b
			_f: 0x6f, // o
			ix,
			_g: 0x2a, // *
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
			"test",
			"test",
			"test",
			"test",
			1,2
		);
		let enc = Bo::encode(&val).unwrap();
		let dec = Bo::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
