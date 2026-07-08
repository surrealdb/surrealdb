//! Storeskey!{ database ID generator state per node
use uuid::Uuid;

use crate::catalog::NamespaceId;
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, key};
use crate::kvs::sequences::SequenceState;

key! {
	/// Key structure for storing database ID generator state.
	///
	/// This key is used to track the state of database ID generation for a specific node
	/// within a namespace. Each node maintains its own state to coordinate with batch
	/// allocations when generating database identifiers.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct DatabaseIdGeneratorStateKey {
		b'/',
		b'*',
		pub ns: NamespaceId,
		b'!',
		b'd',
		b'i',
		pub nid: Uuid,
	}
}

impl_kv_key_storekey!(DatabaseIdGeneratorStateKey => SequenceState);

impl Categorise for DatabaseIdGeneratorStateKey {
	fn categorise(&self) -> Category {
		Category::DatabaseIdentifierState
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = DatabaseIdGeneratorStateKey {
			ns: NamespaceId(123),
			nid: Uuid::from_u128(15),
		};
		let enc = DatabaseIdGeneratorStateKey::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\0\0\0\x7B!di\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x0F");
	}
}
