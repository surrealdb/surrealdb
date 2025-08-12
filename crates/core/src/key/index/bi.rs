//! Stores doc keys for doc_ids
use serde::{Deserialize, Serialize};

use crate::idx::docids::DocId;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use crate::val::RecordId;

/// Id inverted. DocId -> Thing
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Bi<'a> {
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
	pub id: DocId,
}

impl KVKey for Bi<'_> {
	type ValueType = RecordId;
}

impl Categorise for Bi<'_> {
	fn categorise(&self) -> Category {
		Category::IndexDocKeys
	}
}

impl<'a> Bi<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, id: DocId) -> Self {
		Bi {
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
			_f: b'b',
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
		#[rustfmt::skip]
		let val = Bi::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			7
		);
		let enc = Bi::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!bi\0\0\0\0\0\0\0\x07");
	}
}
