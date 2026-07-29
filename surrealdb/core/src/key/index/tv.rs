use std::borrow::Cow;

use surrealdb_strand::TableName;

use crate::catalog::IndexId;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};

key! {
	/// Full-text term-document compaction generation.
	///
	/// This key is intentionally outside the `!tt` delta range. It lets a
	/// compactor validate that the term-doc snapshot it read is still current
	/// before applying exact-key deletes. Missing values are treated as
	/// generation `0`.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Tv<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b't',
		b'v',
	}
}

impl_kv_key_storekey!(Tv<'a> => u64);

impl Categorise for Tv<'_> {
	fn categorise(&self) -> Category {
		Category::IndexTermDocuments
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
		let val = Tv {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
		};
		let enc = Tv::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!tv");
	}
}
