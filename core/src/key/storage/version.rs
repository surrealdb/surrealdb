//! Stores a record document
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct StorageVersion {
	__: u8,
	_a: u8,
}

pub fn new() -> StorageVersion {
	StorageVersion::new()
}

pub fn suffix() -> Vec<u8> {
	vec![b'!', b'v', 0xff]
}

impl Categorise for StorageVersion {
	fn categorise(&self) -> Category {
		Category::StorageVersion
	}
}

impl StorageVersion {
	pub fn new() -> Self {
		Self {
			__: b'!',
			_a: b'v',
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = StorageVersion::new();
		let enc = StorageVersion::encode(&val).unwrap();
		assert_eq!(enc, b"!v");

		let dec = StorageVersion::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
