//! Stores a grant associated with an access method
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct AccessGrantKey<'a> {
		pub prefix: DatabaseRoot,
		b'&',
		pub ac: Cow<'a, str>,
		b'!',
		b'g',
		b'r',
		pub gr: Cow<'a, str>,
	}
}

impl_kv_key_storekey!(AccessGrantKey<'a> => catalog::AccessGrant);

key! {
	pub(crate) struct AccessGrantKeyPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'&',
		pub ac: Cow<'a, str>,
		b'!',
		b'g',
		b'r',
	}
}
impl_kv_range_storekey!(AccessGrantKeyPrefix<'_>);

impl Categorise for AccessGrantKey<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseAccessGrant
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let val = AccessGrantKey {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			ac: "testac".into(),
			gr: "testgr".into(),
		};
		let enc = AccessGrantKey::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02&testac\0!grtestgr\0");
	}

	#[test]
	fn test_prefix() {
		let val = AccessGrantKeyPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			ac: "testac".into(),
		}
		.encode_range()
		.unwrap();
		assert_eq!(val.start.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02&testac\0!gr\0");
		assert_eq!(val.end.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02&testac\0!gs");
	}
}
