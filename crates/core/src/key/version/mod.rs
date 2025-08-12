//! Stores a record document
use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Version {
	__: u8,
	_a: u8,
}

impl KVKey for Version {
	type ValueType = crate::kvs::version::MajorVersion;
}

pub fn new() -> Version {
	Version::new()
}

pub fn proceeding() -> Range<Vec<u8>> {
	vec![b'!', b'v', 0x00]..vec![0xff]
}

impl Categorise for Version {
	fn categorise(&self) -> Category {
		Category::Version
	}
}

impl Version {
	pub fn new() -> Self {
		Self {
			__: b'!',
			_a: b'v',
		}
	}
}

impl Default for Version {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Version::new();
		let enc = Version::encode_key(&val).unwrap();
		assert_eq!(enc, b"!v");
	}
}
