//! Stores the previous value of record for concurrent index building
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::kvs::KVKey;
use crate::kvs::index::PrimaryAppending;
use crate::val::RecordIdKey;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Ip<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	_f: u8,
	_g: u8,
	pub id: RecordIdKey,
}

impl KVKey for Ip<'_> {
	type ValueType = PrimaryAppending;
}

impl<'a> Ip<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: &'a str, id: RecordIdKey) -> Self {
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
	use super::*;

	#[test]
	fn key() {
		let val = Ip::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testix",
			RecordIdKey::String("id".to_string()),
		);
		let enc = Ip::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!ip\0\0\0\x01id\0",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
