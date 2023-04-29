use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Namespace<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
}

pub fn new(ns: &str) -> Namespace<'_> {
	Namespace::new(ns)
}

impl<'a> Namespace<'a> {
	pub fn new(ns: &'a str) -> Self {
		Self {
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
			"test",
		);
		let enc = Namespace::encode(&val).unwrap();
		let dec = Namespace::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
