//! Store appended records for concurrent index building
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::kvs::KVKey;
use crate::kvs::index::Appending;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Ia<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
	pub i: u32,
}

impl KVKey for Ia<'_> {
	type ValueType = Appending;
}

impl<'a> Ia<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: IndexId, i: u32) -> Self {
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
			_g: b'a',
			i,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		let val = Ia::new(NamespaceId(1), DatabaseId(2), "testtb", IndexId(3), 1);
		let enc = Ia::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!ia\x00\x00\x00\x01",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
