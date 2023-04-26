use crate::idx::ft::docids::DocId;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Bk {
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
	pub doc_id: DocId,
}

impl Bk {
	pub fn new(ns: String, db: String, tb: String, ix: String, doc_id: DocId) -> Bk {
		Bk {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0x21, // !
			_e: 0x62, // b
			_f: 0x6b, // k
			ix,
			_g: 0x2a, // *
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
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			7
		);
		let enc = Bk::encode(&val).unwrap();
		let dec = Bk::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
