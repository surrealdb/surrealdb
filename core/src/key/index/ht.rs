//! Stores Things of an HNSW index
use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
use crate::sql::Thing;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Ht<'a> {
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
	pub thing: Thing,
}

impl KeyRequirements for Ht<'_> {
	fn key_category(&self) -> KeyCategory {
		KeyCategory::IndexHnswThings
	}
}

impl<'a> Ht<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, thing: Thing) -> Self {
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
			_f: b'h',
			_g: b't',
			thing,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::syn::Parse;

	#[test]
	fn key() {
		use super::*;
		let val = Ht::new("testns", "testdb", "testtb", "testix", Thing::parse("test:1"));
		let enc = Ht::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0+testix\0!httest\0\0\0\0\0\x80\0\0\0\0\0\0\x01",
			"{}",
			String::from_utf8_lossy(&enc)
		);

		let dec = Ht::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
