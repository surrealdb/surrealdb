//! Stores a DEFINE API definition
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::catalog::{ApiDefinition, DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Ap<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
	pub ap: &'a str,
}

impl KVKey for Ap<'_> {
	type ValueType = ApiDefinition;
}

pub fn new(ns: NamespaceId, db: DatabaseId, ap: &str) -> Ap<'_> {
	Ap::new(ns, db, ap)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!ap\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!ap\xff");
	Ok(k)
}

impl Categorise for Ap<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseApi
	}
}

impl<'a> Ap<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, ap: &'a str) -> Self {
		Self {
			__: b'/', // /
			_a: b'*', // *
			ns,
			_b: b'*', // *
			db,
			_c: b'!', // !
			_d: b'a', // a
			_e: b'p', // p
			ap,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
            let val = Ap::new(
            NamespaceId(1),
            DatabaseId(2),
            "test",
        );
		let enc = Ap::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!aptest\0");
	}

	#[test]
	fn prefix() {
		let val = super::prefix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ap\0");
	}

	#[test]
	fn suffix() {
		let val = super::suffix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ap\xff");
	}
}
