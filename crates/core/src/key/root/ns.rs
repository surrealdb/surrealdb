//! Stores a DEFINE NAMESPACE config definition
use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::NamespaceDefinition;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct NamespaceKey<'key> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub ns: Cow<'key, str>,
}

impl_kv_key_storekey!(NamespaceKey<'_> => NamespaceDefinition);

pub fn new(ns: &str) -> NamespaceKey<'_> {
	NamespaceKey::new(ns)
}

pub fn prefix() -> Vec<u8> {
	let mut k = super::all::kv();
	k.extend_from_slice(b"!ns\x00");
	k
}

pub fn suffix() -> Vec<u8> {
	let mut k = super::all::kv();
	k.extend_from_slice(b"!ns\xff");
	k
}

impl Categorise for NamespaceKey<'_> {
	fn categorise(&self) -> Category {
		Category::Namespace
	}
}

impl<'key> NamespaceKey<'key> {
	pub fn new(ns: &'key str) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'n',
			_c: b's',
			ns: Cow::Borrowed(ns),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = NamespaceKey::new("test");
		let enc = NamespaceKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/!nstest\0");
	}
}
