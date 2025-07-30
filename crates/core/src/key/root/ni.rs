//! Stores namespace ID generator state
use crate::idg::u32::U32;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::KVKey;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Ni {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
}

impl KVKey for Ni {
	type ValueType = U32;
}

impl Default for Ni {
	fn default() -> Self {
		Self::new()
	}
}

impl Categorise for Ni {
	fn categorise(&self) -> Category {
		Category::NamespaceIdentifier
	}
}

impl Ni {
	pub fn new() -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'n',
			_c: b'i',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		let val = Ni::new();
		let enc = Ni::encode_key(&val).unwrap();
		assert_eq!(&enc, b"/!ni");
	}
}
