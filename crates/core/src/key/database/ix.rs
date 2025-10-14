//! Stores the next and available freed IDs for documents
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::idg::u32::U32;
use crate::key::database::all::DatabaseRoot;
use crate::kvs::impl_kv_key_storekey;

// Index ID generator
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct IndexIdGeneratorKey {
	table_root: DatabaseRoot,
	_c: u8,
	_d: u8,
	_e: u8,
}

impl_kv_key_storekey!(IndexIdGeneratorKey => U32);

pub fn new(ns: NamespaceId, db: DatabaseId) -> IndexIdGeneratorKey {
	IndexIdGeneratorKey::new(ns, db)
}

impl IndexIdGeneratorKey {
	pub fn new(ns: NamespaceId, db: DatabaseId) -> Self {
		IndexIdGeneratorKey {
			table_root: DatabaseRoot::new(ns, db),
			_c: b'!',
			_d: b't',
			_e: b'i',
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
		let val = IndexIdGeneratorKey::new(
			NamespaceId(123),
			DatabaseId(234),
		);
		let enc = IndexIdGeneratorKey::encode_key(&val).unwrap();
		assert_eq!(&enc, b"/*\x00\x00\x00\x7b*\x00\x00\x00\xea!\x74\x69");
	}
}
