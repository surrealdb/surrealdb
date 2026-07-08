//! Stores table ID generator state per node
use uuid::Uuid;

use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};
use crate::kvs::sequences::SequenceState;

key! {
/// Key structure for storing table ID generator state.
///
/// This key is used to track the state of table ID generation for a specific node
/// within a database. Each node maintains its own state to coordinate with batch
/// allocations when generating table identifiers.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, )]
	pub(crate) struct TableIdGeneratorStateKey {
		pub prefix: DatabaseRoot,
		b'!',
		b't',
		b'i',
		pub nid: Uuid,
	}
}

impl_kv_key_storekey!(TableIdGeneratorStateKey => SequenceState);

impl Categorise for TableIdGeneratorStateKey {
	fn categorise(&self) -> Category {
		Category::DatabaseTableIdentifierState
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = TableIdGeneratorStateKey {
			prefix: DatabaseRoot {
				ns: NamespaceId(123),
				db: DatabaseId(234),
			},
			nid: Uuid::from_u128(15),
		};
		let enc = TableIdGeneratorStateKey::encode_key(&val).unwrap();
		assert_eq!(
			enc.as_slice(),
			b"/*\x00\x00\x00\x7B*\x00\x00\x00\xEA!ti\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x0F"
		);
	}
}
