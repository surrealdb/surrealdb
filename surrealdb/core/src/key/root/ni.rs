//! Stores namespace ID generator state per node
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::kvs::sequences::SequenceState;

/// Key structure for storing namespace ID generator state.
///
/// This key is used to track the state of namespace ID generation for a specific node
/// at the root level. Each node maintains its own state to coordinate with batch
/// allocations when generating namespace identifiers.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct NamespaceIdGeneratorStateKey {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub nid: Uuid,
}

impl_kv_key_storekey!(NamespaceIdGeneratorStateKey=> SequenceState);

impl Categorise for NamespaceIdGeneratorStateKey {
	fn categorise(&self) -> Category {
		Category::NamespaceIdentifierState
	}
}

impl NamespaceIdGeneratorStateKey {
	/// Creates a new namespace ID generator state key.
	///
	/// # Arguments
	/// * `nid` - The node ID that owns this state
	pub fn new(nid: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'n',
			_c: b'i',
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
		let val = NamespaceIdGeneratorStateKey::new(Uuid::from_u128(1));
		let enc = NamespaceIdGeneratorStateKey::encode_key(&val).unwrap();
		assert_eq!(&enc, b"/!ni\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01");
	}
}
