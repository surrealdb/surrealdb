//! Stores database ID generator state per node
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::catalog::NamespaceId;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::kvs::sequences::SequenceState;

/// Key structure for storing database ID generator state.
///
/// This key is used to track the state of database ID generation for a specific node
/// within a namespace. Each node maintains its own state to coordinate with batch
/// allocations when generating database identifiers.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct DatabaseIdGeneratorStateKey {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	_c: u8,
	_d: u8,
	nid: Uuid,
}

impl_kv_key_storekey!(DatabaseIdGeneratorStateKey => SequenceState);

impl Categorise for DatabaseIdGeneratorStateKey {
	fn categorise(&self) -> Category {
		Category::DatabaseIdentifierState
	}
}
impl DatabaseIdGeneratorStateKey {
	/// Creates a new database ID generator state key.
	///
	/// # Arguments
	/// * `ns` - The namespace ID
	/// * `nid` - The node ID that owns this state
	pub fn new(ns: NamespaceId, nid: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'd',
			_d: b'i',
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
		let val = DatabaseIdGeneratorStateKey::new(
			NamespaceId(123),Uuid::from_u128(15)
		);
		let enc = DatabaseIdGeneratorStateKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\0\0\0\x7B!di\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x0F");
	}
}
