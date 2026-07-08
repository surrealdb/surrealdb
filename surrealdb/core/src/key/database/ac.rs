//! Stores a DEFINE ACCESS ON DATABASE configuration
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::AccessDefinition;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct AccessKey<'a> {
		pub prefix: DatabaseRoot,
		b'!',
		b'a',
		b'c',
		pub ac: Cow<'a, str>,
	}
}
impl_kv_key_storekey!(AccessKey<'a> => AccessDefinition);
impl Categorise for AccessKey<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseAccess
	}
}

key! {
	pub(crate) struct AccessKeyPrefix {
		pub prefix: DatabaseRoot,
		b'!',
		b'a',
		b'c',
	}
}
impl_kv_range_storekey!(AccessKeyPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let val = AccessKey {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			ac: Cow::from("testac"),
		};
		let enc = AccessKey::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!actestac\0");
	}

	#[test]
	fn test_prefix() {
		let val = AccessKeyPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
		}
		.encode_range()
		.unwrap();
		assert_eq!(val.start.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ac\0");
		assert_eq!(val.end.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ad");
	}
}
