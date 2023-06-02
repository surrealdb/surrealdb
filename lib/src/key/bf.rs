use crate::idx::ft::docids::DocId;
use crate::idx::ft::terms::TermId;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Bf<'a> {
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
	pub doc_id: DocId,
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

#[cfg(test)]
mod tests {
	use crate::key::bf::Bf;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Bf::new(
			"test",
			"test",
			"test",
			"test",
			1,
			2
		);
		let enc = Bf::encode(&val).unwrap();
		let dec = Bf::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
