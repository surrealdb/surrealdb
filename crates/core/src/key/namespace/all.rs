//! Stores the key prefix for all keys under a namespace
use serde::{Deserialize, Serialize};

use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct All<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
}

impl KVKey for All<'_> {
	type ValueType = Vec<u8>;
}

pub fn new(ns: &str) -> All<'_> {
	All::new(ns)
}

impl Categorise for All<'_> {
	fn categorise(&self) -> Category {
		Category::NamespaceRoot
	}
}

impl<'a> All<'a> {
	pub fn new(ns: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = All::new(
			"testns",
		);
		let enc = All::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0");
	}
}
