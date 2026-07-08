//! Stores a grant associated with an access method
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog;
use crate::catalog::NamespaceId;
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd,)]
	pub(crate) struct AccessGrantKey<'a> {
		b'/',
		b'*',
		pub ns: NamespaceId,
		b'&',
		pub ac: Cow<'a, str>,
		b'!',
		b'g',
		b'r',
		pub gr: Cow<'a, str>,
	}
}

impl_kv_key_storekey!(AccessGrantKey<'a> => catalog::AccessGrant);

impl Categorise for AccessGrantKey<'_> {
	fn categorise(&self) -> Category {
		Category::NamespaceAccessGrant
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct AccessGrantKeyPrefix<'a> {
		b'/',
		b'*',
		pub ns: NamespaceId,
		b'&',
		pub ac: Cow<'a, str>,
		b'!',
		b'g',
		b'r',
	}
}

impl_kv_range_storekey!(AccessGrantKeyPrefix<'_>);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::NamespaceId;
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let val = AccessGrantKey {
			ns: NamespaceId(1),
			ac: "testac".into(),
			gr: "testgr".into(),
		};
		let enc = AccessGrantKey::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01&testac\0!grtestgr\0");
	}

	#[test]
	fn test_prefix() {
		let val = AccessGrantKeyPrefix {
			ns: NamespaceId(1),
			ac: "testac".into(),
		}
		.encode_range()
		.unwrap();
		assert_eq!(val.start.as_slice(), b"/*\x00\x00\x00\x01&testac\0!gr\0");
		assert_eq!(val.end.as_slice(), b"/*\x00\x00\x00\x01&testac\0!gs");
	}
}
