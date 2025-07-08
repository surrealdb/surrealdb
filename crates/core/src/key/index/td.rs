//! Stores the term/document frequency and offsets

use crate::idx::docids::DocId;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{KeyEncode, impl_key};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Td<'a> {
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
	pub term: &'a str,
	pub id: Option<DocId>,
}
impl_key!(Td<'a>);

impl Categorise for Td<'_> {
	fn categorise(&self) -> Category {
		Category::IndexTermDocument
	}
}

impl<'a> Td<'a> {
	pub(crate) fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		term: &'a str,
		id: Option<DocId>,
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
			_f: b't',
			_g: b'd',
			term,
			id,
		}
	}

	pub(crate) fn range_with_id(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		term: &'a str,
	) -> Result<(Vec<u8>, Vec<u8>)> {
		let prefix = TdPrefix::new(ns, db, tb, ix, term);
		let mut beg = prefix.encode()?;
		beg.extend_from_slice(b"\x01\0\0\0\0\0\0\0\0");
		let mut end = prefix.encode()?;
		end.extend_from_slice(b"\x01\xff\xff\xff\xff\xff\xff\xff\xff");
		Ok((beg, end))
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
struct TdPrefix<'a> {
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
	pub term: &'a str,
}
impl_key!(TdPrefix<'a>);

impl<'a> TdPrefix<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, term: &'a str) -> Self {
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
			_f: b't',
			_g: b'd',
			term,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KeyDecode;

	#[test]
	fn key_with_id() {
		let id = Some(129);
		let val = Td::new("testns", "testdb", "testtb", "testix", "term", id);
		let enc = Td::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!tdterm\0\x01\0\0\0\0\0\0\0\x81");
		let dec = Td::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn key_no_id() {
		let val = Td::new("testns", "testdb", "testtb", "testix", "term", None);
		let enc = Td::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!tdterm\0\0");
		let dec = Td::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn range_with_id() {
		let (beg, end) = Td::range_with_id("testns", "testdb", "testtb", "testix", "term").unwrap();
		assert_eq!(beg, b"/*testns\0*testdb\0*testtb\0+testix\0!tdterm\0\x01\0\0\0\0\0\0\0\0");
		assert_eq!(
			end,
			b"/*testns\0*testdb\0*testtb\0+testix\0!tdterm\0\x01\xff\xff\xff\xff\xff\xff\xff\xff"
		);
	}
}
