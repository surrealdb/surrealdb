//! Stores the next and available freed IDs for Tables
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::kvs::impl_kv_key_storekey;
use crate::kvs::sequences::SequenceState;

// Table ID generator
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct TableIdGeneratorStateKey {
	table_root: DatabaseRoot,
	_c: u8,
	_d: u8,
	_e: u8,
	nid: Uuid,
}

impl_kv_key_storekey!(TableIdGeneratorStateKey => SequenceState);

impl Categorise for TableIdGeneratorStateKey {
	fn categorise(&self) -> Category {
		Category::DatabaseTableIdentifierState
	}
}

impl TableIdGeneratorStateKey {
	pub fn new(ns: NamespaceId, db: DatabaseId, nid: Uuid) -> Self {
		TableIdGeneratorStateKey {
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
		let val = TableIdGeneratorStateKey::new(
			NamespaceId(123),
			DatabaseId(234),
		Uuid::from_u128(15)
		);
		let enc = TableIdGeneratorStateKey::encode_key(&val).unwrap();
		assert_eq!(
			&enc,
			b"/*\x00\x00\x00\x7B*\x00\x00\x00\xEA!ti\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x0F"
		);
	}
}
