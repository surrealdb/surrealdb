//! Stores cluster membership information
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Represents cluster information.
// In the future, this could also include broadcast addresses and other information.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Nd {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	#[serde(with = "uuid::serde::compact")]
	pub nd: Uuid,
}

pub fn new(nd: Uuid) -> Nd {
	Nd::new(nd)
}

pub fn prefix() -> Vec<u8> {
	let mut k = crate::key::root::all::new().encode().unwrap();
	k.extend_from_slice(b"!nd\x00");
	k
}

pub fn suffix() -> Vec<u8> {
	let mut k = crate::key::root::all::new().encode().unwrap();
	k.extend_from_slice(b"!nd\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00");
	k
}

impl Categorise for Nd {
	fn categorise(&self) -> Category {
		Category::Node
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
		let val = super::prefix();
		assert_eq!(val, b"/!nd\0")
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix();
		assert_eq!(val, b"/!nd\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00")
	}
}
