//! Stores the key prefix for all keys under a root access method
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
	pub ac: &'a str,
}
impl_key!(AccessRoot<'a>);

impl KVKey for AccessRoot<'_> {
	type ValueType = Vec<u8>;
}

pub fn new(ac: &str) -> AccessRoot {
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
			"testac",
		);
		let enc = KeyEncode::encode(&val).unwrap();
		assert_eq!(enc, b"/&testac\0");

		let dec = KeyDecode::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
