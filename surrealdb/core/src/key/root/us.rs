//! Stores a DEFINE USER ON ROOT config definition
use std::borrow::Cow;

use crate::catalog;
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Us<'a> {
		b'/',
		b'!',
		b'u',
		b's',
		pub user: Cow<'a, str>,
	}
}

impl_kv_key_storekey!(Us<'a> => catalog::UserDefinition);

impl Categorise for Us<'_> {
	fn categorise(&self) -> Category {
		Category::User
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct UsPrefix {
		b'/',
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
			user: "testuser".into(),
		};
		let enc = Us::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/!ustestuser\x00");
	}

	#[test]
	fn test_prefix() {
		let val = UsPrefix {}.encode_range().unwrap();
		assert_eq!(val.start.as_slice(), b"/!us\0");
		assert_eq!(val.end.as_slice(), b"/!ut");
	}
}
