//! Stores a database ID generator state
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Di {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	_c: u8,
	_d: u8,
}
impl_key!(Di);

pub fn new(ns: u32) -> Di {
	Di::new(ns)
}

impl Categorise for Di {
	fn categorise(&self) -> Category {
		Category::DatabaseIdentifier
	}
}
impl Di {
	pub fn new(ns: u32) -> Self {
		Self {
			__: b'/',
			_a: b'+',
			ns,
			_b: b'!',
			_c: b'd',
			_d: b'i',
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
		let val = Di::new(
			123,
		);
		let enc = Di::encode(&val).unwrap();
		assert_eq!(enc, vec![0x2f, 0x2b, 0, 0, 0, 0x7b, 0x21, 0x64, 0x69]);

		let dec = Di::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
