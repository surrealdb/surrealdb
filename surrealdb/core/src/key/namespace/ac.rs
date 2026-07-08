//! Stores a DEFINE ACCESS ON NAMESPACE configuration
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::{AccessDefinition, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct AccessKey<'a> {
		b'/',
		b'*',
		pub ns: NamespaceId,
		b'!',
		b'a',
		b'c',
		pub ac: Cow<'a, str>,
	}
}

impl_kv_key_storekey!(AccessKey<'a> => AccessDefinition);
impl Categorise for AccessKey<'_> {
	fn categorise(&self) -> Category {
		Category::NamespaceAccess
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct AccessKeyPrefix {
		b'/',
		b'*',
		pub ns: NamespaceId,
		b'!',
		b'a',
		b'c',
	}
}
impl_kv_range_storekey!(AccessKeyPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = AccessKey {
			ns: NamespaceId(1),
			ac: "testac".into(),
		};
		let enc = AccessKey::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01!actestac\0");
	}
}
