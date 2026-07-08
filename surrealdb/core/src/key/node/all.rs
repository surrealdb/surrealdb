//! Stores the key prefix for all nodes
use uuid::Uuid;

use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd,)]
	pub(crate) struct All {
		b'/',
		b'$',
		pub nd: Uuid,
	}
}

impl_kv_key_storekey!(All => Vec<u8>);

impl Categorise for All {
	fn categorise(&self) -> Category {
		Category::NodeRoot
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVKey;

	#[test]
	fn key() {
		let nd = Uuid::from_bytes([
			0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
			0x0f, 0x10,
		]);

		let val = All {
			nd,
		};
		let enc = All::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/$\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10");
	}
}
