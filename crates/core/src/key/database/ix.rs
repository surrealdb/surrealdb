//! Stores the next and available freed IDs for documents
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::table::all::TableRoot;
use crate::kvs::impl_kv_key_storekey;
use crate::kvs::sequences::SequenceState;

// Index ID generator
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct IndexIdGeneratorStateKey<'a> {
	table_root: TableRoot<'a>,
	_c: u8,
	_d: u8,
	_e: u8,
	nid: Uuid,
}

impl_kv_key_storekey!(IndexIdGeneratorStateKey<'_> => SequenceState);

pub fn new(ns: NamespaceId, db: DatabaseId, tb: &str, nid: Uuid) -> IndexIdGeneratorStateKey<'_> {
	IndexIdGeneratorStateKey::new(ns, db, tb, nid)
}

impl<'a> Categorise for IndexIdGeneratorStateKey<'a> {
	fn categorise(&self) -> Category {
		Category::TableIndexIdentifier
	}
}

impl<'a> IndexIdGeneratorStateKey<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, nid: Uuid) -> Self {
		IndexIdGeneratorStateKey {
			table_root: TableRoot::new(ns, db, tb),
			_c: b'!',
			_d: b'i',
			_e: b'x',
			nid,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = IndexIdGeneratorStateKey::new(
			NamespaceId(123),
			DatabaseId(234),
		"testtb",
		Uuid::from_u128(15)
		);
		let enc = IndexIdGeneratorStateKey::encode_key(&val).unwrap();
		assert_eq!(&enc, b"/*\x00\x00\x00\x7b*\x00\x00\x00\xea!\x74\x69");
	}
}
