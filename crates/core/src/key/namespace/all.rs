//! Stores the key prefix for all keys under a namespace
use crate::catalog::NamespaceId;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct All {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
}
impl_key!(All);

pub fn new(ns: NamespaceId) -> All {
	All::new(ns)
}

impl Categorise for All {
	fn categorise(&self) -> Category {
		Category::NamespaceRoot
	}
}

impl All {
	pub fn new(ns: NamespaceId) -> Self {
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
	use crate::kvs::{KeyDecode, KeyEncode};

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = All::new(
			NamespaceId(1),
		);
		let enc = All::encode(&val).unwrap();
		assert_eq!(enc, b"/*1\0");

		let dec = All::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
