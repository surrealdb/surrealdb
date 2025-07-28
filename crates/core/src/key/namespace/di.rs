//! Stores a database ID generator state
use crate::idg::u32::U32;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::KVKey;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Di {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	_c: u8,
	_d: u8,
}

impl KVKey for Di {
	type ValueType = U32;
}

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

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Di::new(
			123,
		);
		let enc = Di::encode_key(&val).unwrap();
		assert_eq!(enc, vec![0x2f, 0x2b, 0, 0, 0, 0x7b, 0x21, 0x64, 0x69]);
	}
}
