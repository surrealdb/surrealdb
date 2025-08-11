//! Stores the DocIds -> Thing of an HNSW index
use crate::idx::trees::hnsw::docs::HnswDocsState;
use crate::kvs::KVKey;
use crate::{idx::docids::DocId, val::RecordIdKey};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct HdRoot<'a> {
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
}

impl KVKey for HdRoot<'_> {
	type ValueType = HnswDocsState;
}

impl<'a> HdRoot<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str) -> Self {
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
			_g: b'd',
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Hd<'a> {
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
	pub doc_id: DocId,
}

impl KVKey for Hd<'_> {
	type ValueType = RecordIdKey;
}

impl<'a> Hd<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, doc_id: DocId) -> Self {
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
			_g: b'd',
			doc_id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn root() {
		let val = HdRoot::new("testns", "testdb", "testtb", "testix");
		let enc = HdRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!hd");
	}

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Hd::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			7
		);
		let enc = Hd::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!hd\0\0\0\0\0\0\0\x07");
	}
}
