use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Bs {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	pub tb: String,
	_d: u8,
	_e: u8,
	_f: u8,
	pub ix: String,
	_g: u8,
	_h: u8,
}

impl Bs {
	pub fn new(ns: String, db: String, tb: String, ix: String) -> Bs {
		Bs {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0x21, // !
			_e: 0x69, // i
			_f: 0x78, // x
			ix,
			_g: 0x62, // b
			_h: 0x73, // s
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Bs::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Bs::encode(&val).unwrap();
		let dec = Bs::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
