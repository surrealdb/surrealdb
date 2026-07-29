//! Stores a DEFINE INDEX config definition
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::{IndexId, StoredIndexDefinition};
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
	pub(crate) struct IndexNameLookupKey<'key> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'key, str>,
		b'!',
		b'i',
		b'l',
		pub ix: IndexId,
	}
}

impl_kv_key_storekey!(IndexNameLookupKey<'a> => String);

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
	pub(crate) struct IndexDefinitionKey<'key> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'key, str>,
		b'!',
		b'i',
		b'x',
		pub ix: Cow<'key, str>,
	}
}
impl_kv_key_storekey!(IndexDefinitionKey<'a> => StoredIndexDefinition);

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
	pub(crate) struct IndexDefinitionPrefix<'key> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'key, str>,
		b'!',
		b'i',
		b'x',
	}
}
impl_kv_range_storekey!(IndexDefinitionPrefix<'_>);

impl Categorise for IndexDefinitionKey<'_> {
	fn categorise(&self) -> Category {
		Category::TableIndex
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = "testtb";
		let val = IndexDefinitionKey {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: tb.into(),
			ix: "testix".into(),
		};
		let enc = IndexDefinitionKey::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\0\0\0\x01*\0\0\0\x02*testtb\0!ixtestix\0");
	}
}
