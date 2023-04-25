use crate::idx::ft::docids::DocId;
use crate::idx::ft::terms::TermId;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Bf {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	pub tb: String,
	_d: u8,
	_e: u8,
	_f: u8,
	pub ix: String,
	_g: u8,
	pub term_id: TermId,
	pub doc_id: DocId,
}

impl Bf {
	pub fn new(
		ns: String,
		db: String,
		tb: String,
		ix: String,
		term_id: TermId,
		doc_id: DocId,
	) -> Bf {
		Bf {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0x21, // !
			_e: 0x62, // b
			_f: 0x78, // x
			ix,
			_g: 0x2a, // *
			term_id,
			doc_id,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct BfPrefix {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	pub tb: String,
	_d: u8,
	_e: u8,
	_f: u8,
	pub ix: String,
	_g: u8,
	term_id: TermId,
}

impl BfPrefix {
	pub fn new(ns: String, db: String, tb: String, ix: String, term_id: TermId) -> BfPrefix {
		BfPrefix {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0x21, // !
			_e: 0x62, // b
			_f: 0x78, // x
			ix,
			_g: 0x2a, // *
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
		let val = Bf::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			1,
			2
		);
		let enc = Bf::encode(&val).unwrap();
		let dec = Bf::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn key_prefix() {
		use super::*;
		#[rustfmt::skip]
			let val = BfPrefix::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			3
		);
		let enc = BfPrefix::encode(&val).unwrap();
		let dec = BfPrefix::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
