use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Nt {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	_c: u8,
	_d: u8,
	pub tk: String,
}

pub fn new(ns: &str, tk: &str) -> Nt {
	Nt::new(ns.to_string(), tk.to_string())
}

pub fn prefix(ns: &str) -> Vec<u8> {
	let mut k = super::namespace::new(ns).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x6e, 0x74, 0x00]);
	k
}

pub fn suffix(ns: &str) -> Vec<u8> {
	let mut k = super::namespace::new(ns).encode().unwrap();
	k.extend_from_slice(&[0x21, 0x6e, 0x74, 0xff]);
	k
}

impl Nt {
	pub fn new(ns: String, tk: String) -> Nt {
		Nt {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x21, // !
			_c: 0x6e, // n
			_d: 0x74, // t
			tk,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Nt::new(
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Nt::encode(&val).unwrap();
		let dec = Nt::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
