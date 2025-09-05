//! Stores Things of an HNSW index
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::kvs::KVKey;
use crate::val::RecordIdKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Hi<'a> {
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
	pub id: RecordIdKey,
}

impl KVKey for Hi<'_> {
	type ValueType = u64;
}

impl<'a> Hi<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: IndexId, id: RecordIdKey) -> Self {
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
	use super::*;

	#[test]
	fn key() {
		let val = Hi::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			IndexId(3),
			RecordIdKey::String("testid".to_string()),
		);
		let enc = Hi::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hi\0\0\0\x01testid\0",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
