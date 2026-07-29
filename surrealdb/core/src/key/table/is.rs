//! Stores index ID generator state per node

use std::borrow::Cow;

use surrealdb_strand::TableName;
use uuid::Uuid;

use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};
use crate::kvs::sequences::SequenceState;

key! {
	/// Key structure for storing index ID generator state.
	///
	/// This key is used to track the state of index ID generation for a specific node
	/// within a table. Each node maintains its own state to coordinate with batch
	/// allocations when generating index identifiers.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct IndexIdGeneratorStateKey<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a,TableName>,
		b'!',
		b'i',
		b's',
		pub nid: Uuid,
	}
}

impl_kv_key_storekey!(IndexIdGeneratorStateKey<'a> => SequenceState);

impl<'a> Categorise for IndexIdGeneratorStateKey<'a> {
	fn categorise(&self) -> Category {
		Category::TableIndexIdentifierState
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = IndexIdGeneratorStateKey {
			prefix: DatabaseRoot {
				ns: NamespaceId(123),
				db: DatabaseId(234),
			},
			tb: Cow::Borrowed(&tb),
			nid: Uuid::from_u128(15),
		};
		let enc = IndexIdGeneratorStateKey::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/*\0\0\0\x7B*\0\0\0\xEA*testtb\0!is\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x0F");
	}
}
