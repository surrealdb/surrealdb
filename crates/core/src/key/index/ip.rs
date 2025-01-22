//! Stores the previous value of record for concurrent index building
use crate::kvs::impl_key;
use crate::sql::Id;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Ip<'a> {
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
impl_key!(Ip<'a>);

impl<'a> Ip<'a> {
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
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
			_f: b'i',
			_g: b'p',
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
		let val = Ip::new("testns", "testdb", "testtb", "testix", Id::from("id".to_string()));
		let enc = Ip::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0+testix\0!ip\0\0\0\x01id\0",
			"{}",
			String::from_utf8_lossy(&enc)
		);

		let dec = Ip::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
