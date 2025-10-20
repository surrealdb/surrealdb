//! Stores the next and available freed IDs for documents
use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::kvs::impl_kv_key_storekey;
use crate::kvs::sequences::SequenceState;
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

// Index ID generator
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct IndexIdGeneratorStateKey {
	table_root: DatabaseRoot,
	_c: u8,
	_d: u8,
	_e: u8,
	nid: Uuid,
}

impl_kv_key_storekey!(IndexIdGeneratorStateKey => SequenceState);

pub fn new(ns: NamespaceId, db: DatabaseId, nid: Uuid) -> IndexIdGeneratorStateKey {
	IndexIdGeneratorStateKey::new(ns, db, nid)
}

impl Categorise for IndexIdGeneratorStateKey {
	fn categorise(&self) -> Category {
		Category::TableIndexIdentifier
	}
}

impl IndexIdGeneratorStateKey {
	pub fn new(ns: NamespaceId, db: DatabaseId, nid: Uuid) -> Self {
		IndexIdGeneratorStateKey {
			table_root: DatabaseRoot::new(ns, db),
			_c: b'!',
			_d: b't',
			_e: b'i',
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
		Uuid::from_u128(15)
		);
		let enc = IndexIdGeneratorStateKey::encode_key(&val).unwrap();
		assert_eq!(&enc, b"/*\x00\x00\x00\x7b*\x00\x00\x00\xea!\x74\x69");
	}
}
