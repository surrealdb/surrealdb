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
	k.extend_from_slice(&[0x21, 0x6e, 0x6c, 0x00]);
	k
}

pub fn suffix(ns: &str) -> Vec<u8> {
	let mut k = super::namespace::new(ns).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x6e, 0x6c, 0xff]);
	k
}

impl<'a> Nl<'a> {
	pub fn new(ns: &'a str, us: &'a str) -> Self {
		Self {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x21, // !
			_c: 0x6e, // n
			_d: 0x6c, // l
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
			"test",
			"test",
		);
		let enc = Nl::encode(&val).unwrap();
		let dec = Nl::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
