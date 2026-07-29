//! Stores a DEFINE ACCESS ON ROOT configuration
use std::borrow::Cow;

use crate::catalog::StoredAccessDefinition;
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct AccessKey<'a> {
		b'/',
		b'!',
		b'a',
		b'c',
		pub ac: Cow<'a, str>,
	}
}

impl_kv_key_storekey!(AccessKey<'a> => StoredAccessDefinition);

impl Categorise for AccessKey<'_> {
	fn categorise(&self) -> Category {
		Category::Access
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct AccessKeyPrefix {
		b'/',
		b'!',
		b'a',
		b'c',
	}
}
impl_kv_range_storekey!(AccessKeyPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let val = AccessKey {
			ac: "testac".into(),
		};
		let enc = AccessKey::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/!actestac\x00");
	}

	#[test]
	fn test_prefix() {
		let val = AccessKeyPrefix {}.encode_range().unwrap();
		assert_eq!(val.start.as_slice(), b"/!ac\0");
		assert_eq!(val.end.as_slice(), b"/!ad");
	}
}
