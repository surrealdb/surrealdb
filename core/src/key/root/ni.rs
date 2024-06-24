//! Stores namespace ID generator state
use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Ni {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
}

impl Default for Ni {
	fn default() -> Self {
		Self::new()
	}
}

impl KeyRequirements for Ni {
	fn key_category(&self) -> KeyCategory {
		KeyCategory::NamespaceIdentifier
	}
}

impl Ni {
	pub fn new() -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'n',
			_c: b'i',
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		let val = Ni::new();
		let enc = Ni::encode(&val).unwrap();
		let dec = Ni::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
