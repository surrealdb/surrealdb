use std::borrow::Cow;

use surrealdb_strand::TableName;

use crate::catalog::IndexId;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};

key! {
	/// Full-text document-stat compaction generation.
	///
	/// This key is intentionally outside the `!dc` root/delta range so that
	/// compaction can guard a short write phase without adding to the scanned
	/// keyspace. Missing values are treated as generation `0`.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Dv<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'd',
		b'v',
	}
}

impl_kv_key_storekey!(Dv<'a> => u64);

impl Categorise for Dv<'_> {
	fn categorise(&self) -> Category {
		Category::IndexFullTextDocCountAndLength
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
		let val = Dv {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
		};
		let enc = Dv::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!dv");
	}
}
