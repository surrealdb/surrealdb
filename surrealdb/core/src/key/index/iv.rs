use std::borrow::Cow;

use surrealdb_strand::TableName;

use crate::catalog::IndexId;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};

key! {
	/// Count-index compaction generation.
	///
	/// This key is intentionally outside the `!iu` count-entry range. It lets a
	/// compactor validate that the count snapshot it read is still current before
	/// deleting exact keys and writing the compacted aggregate. Missing values are
	/// treated as generation `0`.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
		pub(crate) struct Iv<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'i',
		b'v',
	}
}

impl_kv_key_storekey!(Iv<'a> => u64);

impl Categorise for Iv<'_> {
	fn categorise(&self) -> Category {
		Category::IndexCountState
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
		let val = Iv {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
		};
		let enc = Iv::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!iv");
	}
}
