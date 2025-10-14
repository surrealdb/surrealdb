//! Stores the next and available freed IDs for documents
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::database::all::DatabaseRoot;
use crate::kvs::impl_kv_key_storekey;

// Table ID generator
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct TableIdGeneratorKey {
	table_root: DatabaseRoot,
	_c: u8,
	_d: u8,
	_e: u8,
}

impl_kv_key_storekey!(TableIdGeneratorKey => Vec<u8>);

pub fn new(ns: NamespaceId, db: DatabaseId) -> TableIdGeneratorKey {
	TableIdGeneratorKey::new(ns, db)
}

impl TableIdGeneratorKey {
	pub fn new(ns: NamespaceId, db: DatabaseId) -> Self {
		TableIdGeneratorKey {
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
		let val = TableIdGeneratorKey::new(
			NamespaceId(123),
			DatabaseId(234),
		);
		let enc = TableIdGeneratorKey::encode_key(&val).unwrap();
		assert_eq!(&enc, b"/*\x00\x00\x00\x7b*\x00\x00\x00\xea!\x74\x69");
	}
}
