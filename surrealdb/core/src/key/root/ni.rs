//! Stores namespace ID generator state per node
use uuid::Uuid;

use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, key};
use crate::kvs::sequences::SequenceState;

key! {
	/// Key structure for storing namespace ID generator state.
	///
	/// This key is used to track the state of namespace ID generation for a specific node
	/// at the root level. Each node maintains its own state to coordinate with batch
	/// allocations when generating namespace identifiers.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct NamespaceIdGeneratorStateKey {
		b'/',
		b'!',
		b'n',
		b'i',
		pub nid: Uuid,
	}
}

impl_kv_key_storekey!(NamespaceIdGeneratorStateKey=> SequenceState);

impl Categorise for NamespaceIdGeneratorStateKey {
	fn categorise(&self) -> Category {
		Category::NamespaceIdentifierState
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = NamespaceIdGeneratorStateKey {
			nid: Uuid::from_u128(1),
		};
		let enc = NamespaceIdGeneratorStateKey::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/!ni\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01");
	}
}
