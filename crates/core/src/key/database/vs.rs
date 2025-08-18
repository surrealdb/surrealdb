//! Stores database versionstamps
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use crate::vs::VersionStamp;

// Vs stands for Database Versionstamp
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Vs {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
}

impl KVKey for Vs {
	type ValueType = VersionStamp;
}

pub fn new(ns: NamespaceId, db: DatabaseId) -> Vs {
	Vs::new(ns, db)
}

impl Categorise for Vs {
	fn categorise(&self) -> Category {
		Category::DatabaseVersionstamp
	}
}

impl Vs {
	pub fn new(ns: NamespaceId, db: DatabaseId) -> Self {
		Vs {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'v',
			_e: b's',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Vs::new(
			NamespaceId(1),
			DatabaseId(2),
		);
		let enc = Vs::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!vs");
	}
}
