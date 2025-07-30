//! Stores a DEFINE BUCKET definition
use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{KeyEncode, impl_key};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Bu<'a> {
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
impl_key!(Bu<'a>);

pub fn new<'a>(ns: NamespaceId, db: DatabaseId, bu: &'a str) -> Bu<'a> {
	Bu::new(ns, db, bu)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!bu\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!bu\xff");
	Ok(k)
}

impl Categorise for Bu<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseBucket
	}
}

impl<'a> Bu<'a> {
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
	use crate::kvs::KeyDecode;

	#[test]
	fn key() {
		#[rustfmt::skip]
            let val = Bu::new(
            NamespaceId(1),
            DatabaseId(2),
            "test",
        );
		let enc = Bu::encode(&val).unwrap();
		assert_eq!(enc, b"/*1\0*2\0!butest\0");
		let dec = Bu::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn prefix() {
		let val = super::prefix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*1\0*2\0!bu\0");
	}

	#[test]
	fn suffix() {
		let val = super::suffix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*1\0*2\0!bu\xff");
	}
}
