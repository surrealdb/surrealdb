//! Stores the term/document frequency and offsets

use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::impl_key;
use crate::sql::Id;
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
	pub id: Id,
}
impl_key!(Td<'a>);

impl Categorise for Td<'_> {
	fn categorise(&self) -> Category {
		Category::IndexTermDocument
	}
}

impl<'a> Td<'a> {
	pub fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		term: &'a str,
		id: &'a Id,
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
			id: id.to_owned(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
struct PrefixTerm<'a> {
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
impl_key!(PrefixTerm<'a>);

impl<'a> PrefixTerm<'a> {
	fn _new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, term: &'a str) -> Self {
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
	use crate::kvs::{KeyDecode, KeyEncode};
	use crate::syn::Parse;

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let id = Id::String("id".to_string());
		let val = Td::new("testns", "testdb", "testtb", "testix", "term", &id);
		let enc = Td::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!tdterm\0\0\0\0\x01id\0");
		let dec = Td::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn prefix() {
		use super::*;
		#[rustfmt::skip]
		let val = PrefixTerm::new("testns", "testdb", "testtb", "testix", "term");
		let enc = PrefixTerm::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!tdterm\0");
		let dec = PrefixTerm::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
