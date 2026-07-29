//! Stores a DEFINE BUCKET definition
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::StoredBucketDefinition;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct BucketKey<'a> {
		pub prefix: DatabaseRoot,
		b'!', // *
		b'b', // *
		b'u', // *
		pub bu: Cow<'a, str>,
	}
}

impl_kv_key_storekey!(BucketKey<'a> => StoredBucketDefinition);
impl Categorise for BucketKey<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseBucket
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct BucketKeyPrefix {
		pub prefix: DatabaseRoot,
		b'!',
		b'b',
		b'u',
	}
}
impl_kv_range_storekey!(BucketKeyPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let val = BucketKey {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			bu: "test".into(),
		};
		let enc = BucketKey::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!butest\0");
	}

	#[test]
	fn prefix() {
		let val = BucketKeyPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
		}
		.encode_range()
		.unwrap();
		assert_eq!(val.start.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!bu\0");
		assert_eq!(val.end.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!bv");
	}
}
