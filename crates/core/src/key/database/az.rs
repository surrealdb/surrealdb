//! Stores a DEFINE ANALYZER config definition
use crate::catalog::DatabaseId;
use crate::catalog::NamespaceId;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{KeyEncode, impl_key};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Az<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
	pub az: &'a str,
}
impl_key!(Az<'a>);

pub fn new<'a>(ns: NamespaceId, db: DatabaseId, az: &'a str) -> Az<'a> {
	Az::new(ns, db, az)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!az\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!az\xff");
	Ok(k)
}

impl Categorise for Az<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseAnalyzer
	}
}

impl<'a> Az<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, az: &'a str) -> Self {
		Self {
			__: b'/', // /
			_a: b'*', // *
			ns,
			_b: b'*', // *
			db,
			_c: b'!', // !
			_d: b'a', // a
			_e: b'z', // z
			az,
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
            let val = Az::new(
            NamespaceId(1),
            DatabaseId(2),
            "test",
        );
		let enc = Az::encode(&val).unwrap();
		assert_eq!(enc, b"/*1\0*2\0!aztest\0");
		let dec = Az::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn prefix() {
		let val = super::prefix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*1\0*2\0!az\0");
	}

	#[test]
	fn suffix() {
		let val = super::suffix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*1\0*2\0!az\xff");
	}
}
