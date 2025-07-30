//! Stores the key prefix for all keys under a namespace access method
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::KVKey;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct AccessRoot<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub ac: &'a str,
}

impl KVKey for AccessRoot<'_> {
	type ValueType = Vec<u8>;
}

pub fn new<'a>(ns: &'a str, ac: &'a str) -> AccessRoot<'a> {
	AccessRoot::new(ns, ac)
}

impl Categorise for AccessRoot<'_> {
	fn categorise(&self) -> Category {
		Category::NamespaceAccessRoot
	}
}

impl<'a> AccessRoot<'a> {
	pub fn new(ns: &'a str, ac: &'a str) -> Self {
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
			"testns",
			"testac",
		);
		let enc = AccessRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0&testac\0");
	}
}
