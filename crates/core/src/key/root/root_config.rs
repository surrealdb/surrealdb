//! Stores a DEFINE CONFIG definition
use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::ConfigDefinition;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct RootConfig<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub ty: Cow<'a, str>,
}

impl_kv_key_storekey!(RootConfig<'_> => ConfigDefinition);

pub fn new(ty: &str) -> RootConfig<'_> {
	RootConfig::new(ty)
}

impl Categorise for RootConfig<'_> {
	fn categorise(&self) -> Category {
		Category::RootConfig
	}
}

impl<'a> RootConfig<'a> {
	pub fn new(ty: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'c',
			_c: b'g',
			ty: Cow::Borrowed(ty),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = RootConfig::new("testty");
		let enc = RootConfig::encode_key(&val).unwrap();
		assert_eq!(enc, b"/!cgtestty\0");
	}
}
