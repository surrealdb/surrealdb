//! Stores the key prefix for all keys under a root access method
use serde::{Deserialize, Serialize};

use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct AccessRoot<'a> {
	__: u8,
	_a: u8,
	pub ac: &'a str,
}

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
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = AccessRoot::new(
			"testac",
		);
		let enc = AccessRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/&testac\0");
	}
}
