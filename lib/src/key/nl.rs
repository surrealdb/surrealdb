use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Nl {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	_c: u8,
	_d: u8,
	pub us: String,
}

pub fn new(ns: &str, us: &str) -> Nl {
	Nl::new(ns.to_string(), us.to_string())
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

impl Nl {
	pub fn new(ns: String, us: String) -> Nl {
		Nl {
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
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Nl::encode(&val).unwrap();
		let dec = Nl::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
