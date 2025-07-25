//! Stores doc keys for doc_ids
use crate::expr::Id;
use crate::expr::Thing;
use crate::idx::docids::DocId;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::KVKey;
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};

/// TODO: STU: Move this to its own file.
/// Id inverted. DocId -> Id
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) struct Ii<'a> {
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
	pub id: DocId,
}
impl_key!(Ii<'a>);

impl KVKey for Ii<'_> {
	type ValueType = Id;
}

impl Categorise for Ii<'_> {
	fn categorise(&self) -> Category {
		Category::IndexDocKeys
	}
}

impl<'a> Ii<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, id: DocId) -> Self {
		Ii {
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
			_f: b'i',
			_g: b'i',
			id,
		}
	}
}

/// Id inverted. DocId -> Thing
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) struct Bi<'a> {
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
	pub id: DocId,
}
impl_key!(Bi<'a>);

impl KVKey for Bi<'_> {
	type ValueType = Thing;
}

impl Categorise for Bi<'_> {
	fn categorise(&self) -> Category {
		Category::IndexDocKeys
	}
}

impl<'a> Bi<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, id: DocId) -> Self {
		Bi {
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
			_g: b'i',
			id,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::{KeyDecode, KeyEncode};
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Bi::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			7
		);
		let enc = Bi::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!bi\0\0\0\0\0\0\0\x07");

		let dec = Bi::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
