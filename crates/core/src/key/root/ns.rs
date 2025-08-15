//! Stores a DEFINE NAMESPACE config definition
use serde::{Deserialize, Serialize};

use crate::catalog::{NamespaceDefinition, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Ns {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub ns: NamespaceId,
}

impl KVKey for Ns {
	type ValueType = NamespaceDefinition;
}

pub fn new(ns: NamespaceId) -> Ns {
	Ns::new(ns)
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

impl Categorise for Ns {
	fn categorise(&self) -> Category {
		Category::Namespace
	}
}

impl Ns {
	pub fn new(ns: NamespaceId) -> Self {
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
		let val = Ns::new(NamespaceId(1));
		let enc = Ns::encode_key(&val).unwrap();
		assert_eq!(enc, b"/!ns\x00\x00\x00\x01");
	}
}
