//! Stores the key prefix for all keys under a root access method
use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct AccessRoot<'a> {
	__: u8,
	_a: u8,
	pub ac: Cow<'a, str>,
}

impl_kv_key_storekey!(AccessRoot<'_> => Vec<u8>);

pub fn new(ac: &str) -> AccessRoot<'_> {
	AccessRoot::new(ac)
}

impl Categorise for AccessRoot<'_> {
	fn categorise(&self) -> Category {
		Category::AccessRoot
	}
}

impl<'a> AccessRoot<'a> {
	pub fn new(ac: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'&',
			ac: Cow::Borrowed(ac),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = AccessRoot::new("testac");
		let enc = AccessRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/&testac\0");
	}
}
