//! Stores the key prefix for all keys
use storekey::{BorrowDecode, Encode};

use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[allow(unused)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct All {
	__: u8,
}

impl_kv_key_storekey!(All => ());

pub fn kv() -> Vec<u8> {
	vec![b'/']
}

impl Default for All {
	fn default() -> Self {
		Self::new()
	}
}

impl Categorise for All {
	fn categorise(&self) -> Category {
		Category::Root
	}
}

impl All {
	pub fn new() -> All {
		All {
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
		#[rustfmt::skip]
		let val = All::new();
		let enc = All::encode_key(&val).unwrap();
		assert_eq!(enc, b"/");
	}
}
