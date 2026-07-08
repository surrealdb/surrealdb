//! Stores a DEFINE USER ON DATABASE config definition
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct UserKey<'key> {
		pub prefix: DatabaseRoot,
		b'!',
		b'u',
		b's',
		pub user: Cow<'key, str>,
	}
}
impl_kv_key_storekey!(UserKey<'a> => catalog::UserDefinition);

impl Categorise for UserKey<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseUser
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct UserKeyPrefix {
		pub prefix: DatabaseRoot,
		b'!',
		b'u',
		b's',
	}
}
impl_kv_range_storekey!(UserKeyPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let val = UserKey {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			user: Cow::Borrowed("testuser"),
		};
		let enc = UserKey::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ustestuser\0");
	}

	#[test]
	fn test_prefix() {
		let val = UserKeyPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
		}
		.encode_range()
		.unwrap();
		assert_eq!(val.start.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!us\0");
		assert_eq!(val.end.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ut");
	}
}
