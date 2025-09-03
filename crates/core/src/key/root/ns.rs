//! Stores a DEFINE NAMESPACE config definition
use serde::{Deserialize, Serialize};

use crate::catalog::NamespaceDefinition;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct NamespaceKey<'key> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub ns: &'key str,
}

impl KVKey for NamespaceKey<'_> {
	type ValueType = NamespaceDefinition;
}

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
			ns,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		let val = NamespaceKey::new("test");
		let enc = NamespaceKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/!nstest\0");
	}
}
