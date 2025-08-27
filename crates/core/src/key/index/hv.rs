//! Stores Things of an HNSW index
use std::fmt::Debug;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::idx::trees::hnsw::docs::ElementDocs;
use crate::idx::trees::vector::SerializedVector;
use crate::kvs::KVKey;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Hv<'a> {
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
	pub vec: Arc<SerializedVector>,
}

impl KVKey for Hv<'_> {
	type ValueType = ElementDocs;
}

impl<'a> Hv<'a> {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		ix: &'a str,
		vec: Arc<SerializedVector>,
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
			_g: b'v',
			vec,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		let val = Hv::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testix",
			Arc::new(SerializedVector::I16(vec![2])),
		);
		let enc = Hv::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!hv\0\0\0\x04\x80\x02\x01",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
