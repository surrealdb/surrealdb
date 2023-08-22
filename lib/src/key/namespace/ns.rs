/// Stores a database ID generator state
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Ns {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	_c: u8,
	_d: u8,
}

pub fn new(ns: u32) -> Ns {
	Ns::new(ns)
}

impl Ns {
	pub fn new(ns: u32) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'n',
			_d: b's',
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ns::new(
			1,
		);
		let enc = Ns::encode(&val).unwrap();
		assert_eq!(enc, vec![b'/', b'*', 0, 0, 0, 0, 0, 0, 0, 0x1, b'!', b'n', b's']);

		let dec = Ns::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
