//! Stores a DEFINE USER ON NAMESPACE config definition
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::{self, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Us<'a> {
		b'/',
		b'*',
		pub ns: NamespaceId,
		b'!',
		b'u',
		b's',
		pub user: Cow<'a, str>,
	}
}
impl_kv_key_storekey!(Us<'a> => catalog::UserDefinition);

impl Categorise for Us<'_> {
	fn categorise(&self) -> Category {
		Category::NamespaceUser
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct UsPrefix {
		b'/',
		b'*',
		pub ns: NamespaceId,
		b'!',
		b'u',
		b's',
	}
}
impl_kv_range_storekey!(UsPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let val = Us {
			ns: NamespaceId(1),
			user: "testuser".into(),
		};
		let enc = Us::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01!ustestuser\0");
	}

	#[test]
	fn test_prefix() {
		let val = UsPrefix {
			ns: NamespaceId(1),
		}
		.encode_range()
		.unwrap();
		assert_eq!(val.start.as_slice(), b"/*\x00\x00\x00\x01!us\0");
		assert_eq!(val.end.as_slice(), b"/*\x00\x00\x00\x01!ut");
	}
}
