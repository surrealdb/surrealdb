//! Stores the key prefix for all keys
use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
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

impl KeyRequirements for Kv {
	fn key_category(&self) -> KeyCategory {
		KeyCategory::Root
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
		assert_eq!(enc, b"/");

		let dec = Kv::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
