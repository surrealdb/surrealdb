use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Namespace {
	__: u8,
	_a: u8,
	pub ns: String,
}

pub fn new(ns: &str) -> Namespace {
	Namespace::new(ns.to_string())
}

impl Namespace {
	pub fn new(ns: String) -> Namespace {
		Namespace {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Namespace::new(
			"test".to_string(),
		);
		let enc = Namespace::encode(&val).unwrap();
		let dec = Namespace::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
