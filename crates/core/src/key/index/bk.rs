//! Stores the term list for doc_ids
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::idx::docids::DocId;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Bk<'a> {
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
	pub doc_id: DocId,
}

impl KVKey for Bk<'_> {
	type ValueType = RoaringTreemap;
}

impl Categorise for Bk<'_> {
	fn categorise(&self) -> Category {
		Category::IndexTermList
	}
}

impl<'a> Bk<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: &'a str, doc_id: DocId) -> Self {
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
			_f: b'b',
			_g: b'k',
			doc_id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Bk::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testix",
			7
		);
		let enc = Bk::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!bk\0\0\0\0\0\0\0\x07"
		);
	}
}
