//! Stores doc keys for doc_ids
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::idx::docids::DocId;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use crate::val::RecordIdKey;

/// Id inverted. DocId -> Id
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Ii<'a> {
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
	pub id: DocId,
}

impl KVKey for Ii<'_> {
	type ValueType = RecordIdKey;
}

impl Categorise for Ii<'_> {
	fn categorise(&self) -> Category {
		Category::IndexDocKeys
	}
}

impl<'a> Ii<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: &'a str, id: DocId) -> Self {
		Ii {
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
		let val = Ii::new(NamespaceId(1), DatabaseId(2), "testtb", "testix", 1);
		let enc = Ii::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!ii\0\0\0\0\0\0\0\x01"
		);
	}
}
