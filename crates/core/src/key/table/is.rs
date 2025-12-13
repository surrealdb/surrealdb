//! Stores index ID generator state per node

use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::table::all::TableRoot;
use crate::kvs::impl_kv_key_storekey;
use crate::kvs::sequences::SequenceState;
use crate::val::TableName;

/// Key structure for storing index ID generator state.
///
/// This key is used to track the state of index ID generation for a specific node
/// within a table. Each node maintains its own state to coordinate with batch
/// allocations when generating index identifiers.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
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
	/// Creates a new index ID generator state key.
	///
	/// # Arguments
	/// * `ns` - The namespace ID
	/// * `db` - The database ID
	/// * `tb` - The table name
	/// * `nid` - The node ID that owns this state
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, nid: Uuid) -> Self {
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
	fn key() {
		#[rustfmt::skip]
		let val = IndexIdGeneratorStateKey::new(
			NamespaceId(123),
			DatabaseId(234),
		"testtb",
		Uuid::from_u128(15)
		);
		let enc = IndexIdGeneratorStateKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\0\0\0\x7B*\0\0\0\xEA*testtb\0!is\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x0F");
	}
}
