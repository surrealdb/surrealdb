//! Stores table ID generator state per node
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::kvs::impl_kv_key_storekey;
use crate::kvs::sequences::SequenceState;

/// Key structure for storing table ID generator state.
///
/// This key is used to track the state of table ID generation for a specific node
/// within a database. Each node maintains its own state to coordinate with batch
/// allocations when generating table identifiers.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct TableIdGeneratorStateKey {
	database_root: DatabaseRoot,
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
	/// Creates a new table ID generator state key.
	///
	/// # Arguments
	/// * `ns` - The namespace ID
	/// * `db` - The database ID
	/// * `nid` - The node ID that owns this state
	pub fn new(ns: NamespaceId, db: DatabaseId, nid: Uuid) -> Self {
		TableIdGeneratorStateKey {
			database_root: DatabaseRoot::new(ns, db),
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
