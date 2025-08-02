//! Stores the next and available freed IDs for documents
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

use serde::{Deserialize, Serialize};

// Table ID generator
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Ti {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	_d: u8,
	_e: u8,
}

impl KVKey for Ti {
	type ValueType = Vec<u8>;
}

pub fn new(ns: u32, db: u32) -> Ti {
	Ti::new(ns, db)
}

impl Categorise for Ti {
	fn categorise(&self) -> Category {
		Category::DatabaseTableIdentifier
	}
}

impl Ti {
	pub fn new(ns: u32, db: u32) -> Self {
		Ti {
			__: b'/',
			_a: b'+',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b't',
			_e: b'i',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Ti::new(
			123u32,
			234u32,
		);
		let enc = Ti::encode_key(&val).unwrap();
		// [47, 43, 0, 0, 0, 123, 42, 0, 0, 0, 234, 33, 116, 105]
		assert_eq!(&enc, b"/+\x00\x00\x00\x7b*\x00\x00\x00\xea!\x74\x69");
	}
}
