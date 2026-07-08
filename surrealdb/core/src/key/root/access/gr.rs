//! Stores a grant associated with an access method
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog;
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct AccessGrantKey<'a> {
		b'/',
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
		Category::AccessGrant
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct AccessGrantPrefix<'a> {
		b'/',
		b'&',
		pub ac: Cow<'a, str>,
		b'!',
		b'g',
		b'r',
	}
}
impl_kv_range_storekey!(AccessGrantPrefix<'_>);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let val = AccessGrantKey {
			ac: "testac".into(),
			gr: "testgr".into(),
		};
		let enc = AccessGrantKey::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/&testac\0!grtestgr\0");
	}

	#[test]
	fn test_prefix() {
		let val = AccessGrantPrefix {
			ac: "testac".into(),
		}
		.encode_range()
		.unwrap();
		assert_eq!(&*val.start, b"/&testac\0!gr\0");
		assert_eq!(&*val.end, b"/&testac\0!gs");
	}
}
