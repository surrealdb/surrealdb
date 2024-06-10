//! Stores the key prefix for all keys under a namespace
use crate::key::category::Category;
use crate::key::key_req::KeyRequirements;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct All<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
}

pub fn new(ns: &str) -> All<'_> {
	All::new(ns)
}

impl KeyRequirements for All<'_> {
	fn key_category(&self) -> Category {
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
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = All::new(
			"testns",
		);
		let enc = All::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0");

		let dec = All::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
