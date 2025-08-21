//! Stores the key prefix for all keys
use serde::{Deserialize, Serialize};

use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Kv {
	__: u8,
}

impl KVKey for Kv {
	type ValueType = ();
}

pub fn kv() -> Vec<u8> {
	vec![b'/']
}

impl Default for Kv {
	fn default() -> Self {
		Self::new()
	}
}

impl Categorise for Kv {
	fn categorise(&self) -> Category {
		Category::Root
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
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Kv::new();
		let enc = Kv::encode_key(&val).unwrap();
		assert_eq!(enc, b"/");
	}
}
