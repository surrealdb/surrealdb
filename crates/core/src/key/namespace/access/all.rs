//! Stores the key prefix for all keys under a namespace access method
use serde::{Deserialize, Serialize};

use crate::catalog::NamespaceId;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct AccessRoot<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub ac: &'a str,
}

impl KVKey for AccessRoot<'_> {
	type ValueType = Vec<u8>;
}

pub fn new(ns: NamespaceId, ac: &str) -> AccessRoot<'_> {
	AccessRoot::new(ns, ac)
}

impl Categorise for AccessRoot<'_> {
	fn categorise(&self) -> Category {
		Category::NamespaceAccessRoot
	}
}

impl<'a> AccessRoot<'a> {
	pub fn new(ns: NamespaceId, ac: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'&',
			ac,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = AccessRoot::new(
			NamespaceId(1),
			"testac",
		);
		let enc = AccessRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01&testac\0");
	}
}
