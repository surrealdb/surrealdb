//! Stores the doc length
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::impl_key;
use crate::sql::Id;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Dl<'a> {
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
	pub id: Id,
}
impl_key!(Dl<'a>);

impl Categorise for Dl<'_> {
	fn categorise(&self) -> Category {
		Category::IndexDocLength
	}
}

impl<'a> Dl<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, id: &'a Id) -> Self {
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
			_f: b'd',
			_g: b'l',
			id: id.to_owned(),
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
		let id = Id::String("id".to_string());
		let val = Dl::new("testns", "testdb", "testtb", "testix", &id);
		let enc = Dl::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!dl\0\0\0\x01id\0");

		let dec = Dl::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
