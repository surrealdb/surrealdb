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

impl<'a> Categorise for IndexIdGeneratorStateKey<'a> {
	fn categorise(&self) -> Category {
		Category::TableIndexIdentifierState
	}
}

impl<'a> IndexIdGeneratorStateKey<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, nid: Uuid) -> Self {
		IndexIdGeneratorStateKey {
			table_root: TableRoot::new(ns, db, tb),
			_c: b'!',
			_d: b'i',
			_e: b's',
			nid,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn state_key() {
		#[rustfmt::skip]
		let val = IndexIdGeneratorStateKey::new(
			NamespaceId(123),
			DatabaseId(234),
		"testtb",
		Uuid::from_u128(15)
		);
		let enc = IndexIdGeneratorStateKey::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			vec![
				47, 42, 0, 0, 0, 123, 42, 0, 0, 0, 234, 42, 116, 101, 115, 116, 116, 98, 0, 33,
				105, 115, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15
			]
		);
	}
}
