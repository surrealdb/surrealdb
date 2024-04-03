//! Stores the key prefix for all nodes
use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
use derive::Key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct All {
	__: u8,
	_a: u8,
	#[serde(with = "uuid::serde::compact")]
	pub nd: Uuid,
}

pub fn new(nd: Uuid) -> All {
	All::new(nd)
}

impl KeyRequirements for All {
	fn key_category(&self) -> KeyCategory {
		KeyCategory::NodeRoot
	}
}

impl All {
	pub fn new(nd: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'$',
			nd,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let nd = Uuid::from_bytes([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10]);
		#[rustfmt::skip]
		let val = All::new(
			nd,
		);
		let enc = All::encode(&val).unwrap();
		assert_eq!(enc, b"/$\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10");

		let dec = All::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
