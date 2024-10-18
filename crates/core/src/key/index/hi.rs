//! Stores Things of an HNSW index
use crate::sql::Id;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Hi<'a> {
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

impl<'a> Hi<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, id: Id) -> Self {
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
			_g: b'i',
			id,
		}
	}
}

#[cfg(test)]
mod tests {

	#[test]
	fn key() {
		use super::*;
		let val = Hi::new("testns", "testdb", "testtb", "testix", Id::String("testid".to_string()));
		let enc = Hi::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0+testix\0!hi\0\0\0\x01testid\0",
			"{}",
			String::from_utf8_lossy(&enc)
		);

		let dec = Hi::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
