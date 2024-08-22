//! Stores a record document
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct StorageVersion<'a> {
	__: u8,
	_a: u8,
	_b: u8,
}

pub fn new<'a>() -> StorageVersion<'a> {
	StorageVersion::new()
}

impl Categorise for StorageVersion<'_> {
	fn categorise(&self) -> Category {
		Category::StorageVersion
	}
}

impl<'a> StorageVersion<'a> {
	pub fn new() -> Self {
		Self {
			__: b'/',
			_a: b's',
			_b: b'v',
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
		assert_eq!(enc, b"/sv");

		let dec = StorageVersion::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
