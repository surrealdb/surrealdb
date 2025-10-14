//! Stores a record document
use std::ops::Range;

use storekey::{BorrowDecode, Encode};

use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Version {
	__: u8,
	_a: u8,
}

impl_kv_key_storekey!(Version => crate::kvs::version::MajorVersion);

pub fn new() -> Version {
	Version::new()
}

pub fn proceeding() -> Range<Vec<u8>> {
	vec![b'!', b'v', 0x00]..vec![0xff]
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
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Version::new();
		let enc = Version::encode_key(&val).unwrap();
		assert_eq!(enc, b"!v");
	}
}
