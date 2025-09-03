//! Stores a DEFINE BUCKET definition
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::catalog::{BucketDefinition, DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct BucketKey<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
	pub bu: &'a str,
}

impl KVKey for BucketKey<'_> {
	type ValueType = BucketDefinition;
}

pub fn new(ns: NamespaceId, db: DatabaseId, bu: &str) -> BucketKey<'_> {
	BucketKey::new(ns, db, bu)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!bu\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!bu\xff");
	Ok(k)
}

impl Categorise for BucketKey<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseBucket
	}
}

impl<'a> BucketKey<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, bu: &'a str) -> Self {
		Self {
			__: b'/', // /
			_a: b'*', // *
			ns,
			_b: b'*', // *
			db,
			_c: b'!', // !
			_d: b'b', // b
			_e: b'u', // u
			bu,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
            let val = BucketKey::new(
            NamespaceId(1),
            DatabaseId(2),
            "test",
        );
		let enc = BucketKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!butest\0");
	}

	#[test]
	fn prefix() {
		let val = super::prefix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!bu\0");
	}

	#[test]
	fn suffix() {
		let val = super::suffix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!bu\xff");
	}
}
