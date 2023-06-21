use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Kv {
	__: u8,
}

pub fn new() -> Kv {
	Kv::new()
}

impl Default for Kv {
	fn default() -> Self {
		Self::new()
	}
}

impl Kv {
	pub fn new() -> Kv {
		Kv {
			__: b'/',
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Kv::new();
		let enc = Kv::encode(&val).unwrap();
		let dec = Kv::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
