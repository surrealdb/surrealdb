use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Nl<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	_c: u8,
	_d: u8,
	pub us: &'a str,
}

pub fn new<'a>(ns: &'a str, us: &'a str) -> Nl<'a> {
	Nl::new(ns, us)
}

pub fn prefix(ns: &str) -> Vec<u8> {
	let mut k = super::namespace::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b'n', b'l', 0x00]);
	k
}

pub fn suffix(ns: &str) -> Vec<u8> {
	let mut k = super::namespace::new(ns).encode().unwrap();
	k.extend_from_slice(&[b'!', b'n', b'l', 0xff]);
	k
}

impl<'a> Nl<'a> {
	pub fn new(ns: &'a str, us: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'n',
			_d: b'l',
			us,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Nl::new(
			"testns",
			"testus",
		);
		let enc = Nl::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0!nltestus\0");

		let dec = Nl::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
