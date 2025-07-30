//! Stores a DEFINE API definition
use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{KeyEncode, impl_key};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Ap<'a> {
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
impl_key!(Ap<'a>);

pub fn new<'a>(ns: NamespaceId, db: DatabaseId, ap: &'a str) -> Ap<'a> {
	Ap::new(ns, db, ap)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!ap\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode()?;
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
	use crate::kvs::KeyDecode;

	#[test]
	fn key() {
		#[rustfmt::skip]
            let val = Ap::new(
            NamespaceId(1),
            DatabaseId(2),
            "test",
        );
		let enc = Ap::encode(&val).unwrap();
		assert_eq!(enc, b"/*1\0*2\0!aptest\0");
		let dec = Ap::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn prefix() {
		let val = super::prefix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*1\0*2\0!ap\0");
	}

	#[test]
	fn suffix() {
		let val = super::suffix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*1\0*2\0!ap\xff");
	}
}
