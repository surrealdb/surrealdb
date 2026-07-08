//! key!{Stores a DEFINE DATABASE config definition
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::{DatabaseDefinition, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct DatabaseKey<'key> {
		b'/',
		b'*',
		pub ns: NamespaceId,
		b'!',
		b'd',
		b'b',
		pub db: Cow<'key, str>,
	}
}
impl_kv_key_storekey!(DatabaseKey<'a> => DatabaseDefinition);

impl Categorise for DatabaseKey<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseAlias
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct DatabasePrefix {
		b'/',
		b'*',
		pub ns: NamespaceId,
		b'!',
		b'd',
		b'b',
	}
}
impl_kv_range_storekey!(DatabasePrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let val = DatabaseKey {
			ns: NamespaceId(1),
			db: "test".into(),
		};
		let enc = DatabaseKey::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01!dbtest\0");
	}

	#[test]
	fn test_prefix() {
		let range = DatabasePrefix {
			ns: NamespaceId(1),
		}
		.encode_range()
		.unwrap();
		assert_eq!(range.start.as_slice(), b"/*\x00\x00\x00\x01!db\0");
		assert_eq!(range.end.as_slice(), b"/*\x00\x00\x00\x01!dc");
	}
}
