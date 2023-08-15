//! Stores the next and available freed IDs for documents
use derive::Key;
use serde::{Deserialize, Serialize};

// Table ID generator
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Ti {
	__: u8,
	_a: u8,
	pub ns: u64,
	_b: u8,
	pub db: u64,
	_c: u8,
	_d: u8,
	_e: u8,
}

pub fn new(ns: u64, db: u64) -> Ti {
	Ti::new(ns, db)
}

impl Ti {
	pub fn new(ns: u64, db: u64) -> Self {
		Ti {
			__: b'/',
			_a: b'+',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b't',
			_e: b'i',
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ti::new(
			123u64,
			234u64,
		);
		let enc = Ti::encode(&val).unwrap();
		let dec = Ti::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
