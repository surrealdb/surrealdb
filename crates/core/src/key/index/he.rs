//! Stores Vector of an HNSW index
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::vector::SerializedVector;
use crate::kvs::KVKey;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct He<'a> {
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
	pub element_id: ElementId,
}

impl KVKey for He<'_> {
	type ValueType = SerializedVector;
}

impl<'a> He<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, element_id: ElementId) -> Self {
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
			_g: b'e',
			element_id,
		}
	}
}

#[cfg(test)]
mod tests {

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = He::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			7
		);
		let enc = He::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!he\0\0\0\0\0\0\0\x07");
	}
}
