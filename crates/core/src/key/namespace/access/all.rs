//! Stores the key prefix for all keys under a namespace access method
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::KVKey;
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) struct AccessRoot<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub ac: &'a str,
}
impl_key!(AccessRoot<'a>);

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
	use crate::kvs::{KeyDecode, KeyEncode};
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = AccessRoot::new(
			"testns",
			"testac",
		);
		let enc = AccessRoot::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0&testac\0");

		let dec = AccessRoot::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
