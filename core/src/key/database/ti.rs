//! Stores the next and available freed IDs for documents
use crate::key::category::Category;
use crate::key::key_req::KeyRequirements;
use derive::Key;
use serde::{Deserialize, Serialize};

// Table ID generator
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Ti {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	_d: u8,
	_e: u8,
}

pub fn new(ns: u32, db: u32) -> Ti {
	Ti::new(ns, db)
}

impl KeyRequirements for Ti {
	fn key_category(&self) -> Category {
		Category::DatabaseTableIdentifier
	}
}

impl Ti {
	pub fn new(ns: u32, db: u32) -> Self {
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
			123u32,
			234u32,
		);
		let enc = Ti::encode(&val).unwrap();
		let dec = Ti::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
