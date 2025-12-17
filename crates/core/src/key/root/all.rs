//! Stores the key prefix for all keys
use storekey::{BorrowDecode, Encode};

use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[allow(unused)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Kv {
	__: u8,
}

impl_kv_key_storekey!(Kv => ());

pub fn kv() -> Vec<u8> {
	vec![b'/']
}

impl Default for Kv {
	fn default() -> Self {
		Self::new()
	}
}

impl Categorise for Kv {
	fn categorise(&self) -> Category {
		Category::Root
	}
}

impl Kv {
	pub fn new() -> Kv {
		Kv {
			__: b'/',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = Kv::new();
		let enc = Kv::encode_key(&val).unwrap();
		assert_eq!(enc, b"/");
	}
}
