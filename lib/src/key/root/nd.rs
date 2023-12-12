//! Stores cluster membership information
use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
use crate::kvs::KeyStack;
use derive::Key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const SIZE: usize = 4 + 16;

// Represents cluster information.
// In the future, this could also include broadcast addresses and other information.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Nd {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	#[serde(with = "uuid::serde::compact")]
	pub nd: Uuid,
}

impl KeyRequirements for Nd {
	fn key_category(&self) -> KeyCategory {
		KeyCategory::Node
	}
}

impl Nd {
	pub fn new(nd: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'n',
			_c: b'd',
			nd,
		}
	}

	pub fn prefix() -> KeyStack<SIZE> {
		let mut k = crate::key::root::all::new().encode().unwrap();
		k.extend_from_slice(&[b'!', b'n', b'd', 0x00]);
		KeyStack::<SIZE>::from(&k) // TODO
	}

	pub fn suffix() -> KeyStack<SIZE> {
		let mut k = crate::key::root::all::new().encode().unwrap();
		k.extend_from_slice(&[b'!', b'n', b'd', 0xff]);
		KeyStack::<SIZE>::from(&k) // TODO
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		let val = Nd::new(Uuid::default());
		let enc = Nd::encode(&val).unwrap();
		let dec = Nd::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::Nd::prefix();
		assert_eq!(val, b"/!nd\0")
	}

	#[test]
	fn test_suffix() {
		let val = super::Nd::suffix();
		assert_eq!(val, b"/!nd\xff")
	}
}
