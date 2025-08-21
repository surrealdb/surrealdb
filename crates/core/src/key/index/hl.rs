//! Store and chunked layers of an HNSW index
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::kvs::KVKey;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Hl<'a> {
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
	pub layer: u16,
	pub chunk: u32,
}

impl KVKey for Hl<'_> {
	type ValueType = Vec<u8>;
}

impl<'a> Hl<'a> {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		ix: &'a str,
		layer: u16,
		chunk: u32,
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
			_f: b'h',
			_g: b'l',
			layer,
			chunk,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		let val = Hl::new(NamespaceId(1), DatabaseId(2), "testtb", "testix", 7, 8);
		let enc = Hl::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!hl\0\x07\0\0\0\x08",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
