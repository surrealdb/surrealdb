//! Store the numeric DocId for a given Id
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Id<'a> {
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
	pub id: crate::expr::Id,
}
impl_key!(Id<'a>);

impl Categorise for Id<'_> {
	fn categorise(&self) -> Category {
		Category::IndexInvertedDocIds
	}
}

impl<'a> Id<'a> {
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, id: crate::expr::Id) -> Self {
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
			_f: b'i',
			_g: b'd',
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
		let val = Id::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			crate::expr::Id::from("id".to_string()),
		);
		let enc = Id::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0+testix\0!id\0\0\0\x01id\0",
			"{}",
			String::from_utf8_lossy(&enc)
		);

		let dec = Id::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
