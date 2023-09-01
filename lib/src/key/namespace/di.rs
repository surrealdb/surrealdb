/// Stores a database ID generator state
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Di {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	_c: u8,
	_d: u8,
}

pub fn new(ns: u32) -> Di {
	Di::new(ns)
}

impl Di {
	pub fn new(ns: u32) -> Self {
		Self {
			__: b'/',
			_a: b'*',
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
			1,
		);
		let enc = Di::encode(&val).unwrap();
		assert_eq!(enc, b"/*\0\0\0\x01!di");

		let dec = Di::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
