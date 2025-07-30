//! Stores the key prefix for all keys under a database

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct All {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
}
impl_key!(All);

pub fn new(ns: NamespaceId, db: DatabaseId) -> All {
	All::new(ns, db)
}

impl Categorise for All {
	fn categorise(&self) -> Category {
		Category::DatabaseRoot
	}
}

impl All {
	pub fn new(ns: NamespaceId, db: DatabaseId) -> Self {
		Self {
			__: b'/', // /
			_a: b'*', // *
			ns,
			_b: b'*', // *
			db,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::{KeyDecode, KeyEncode};

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = All::new(
			NamespaceId(1),
			DatabaseId(2),
		);
		let enc = All::encode(&val).unwrap();
		assert_eq!(enc, b"/*1\0*2\0");

		let dec = All::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
