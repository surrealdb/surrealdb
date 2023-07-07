use crate::idx::ft::docids::DocId;
use crate::idx::ft::terms::TermId;
use derive::Key;
use serde::{Deserialize, Serialize};
use std::ops::Range;

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
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'!',
			_e: b'b',
			_f: b'f',
			ix,
			_g: b'*',
			term_id,
			doc_id,
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
			_f: b'f',
			ix,
			_g: b'*',
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
			"testns",
			"testdb",
			"testtb",
			"testix",
			7,
			13
		);
		let enc = Bf::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0!bftestix\0*\
		    \0\0\0\0\0\0\0\x07\
		    \0\0\0\0\0\0\0\x0d"
		);

		let dec = Bf::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
