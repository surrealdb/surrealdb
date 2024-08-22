//! Stores a record document
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};
use std::ops::Range;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Version {
	__: u8,
	_a: u8,
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
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Version::new();
		let enc = Version::encode(&val).unwrap();
		assert_eq!(enc, b"!v");

		let dec = Version::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
