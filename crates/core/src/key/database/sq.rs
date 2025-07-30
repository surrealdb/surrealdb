//! Stores a DEFINE SEQUENCE config definition
use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{KeyEncode, impl_key};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Sq<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
	pub sq: &'a str,
}
impl_key!(Sq<'a>);
pub fn prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"*sq\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"*sq\xff");
	Ok(k)
}

impl Categorise for Sq<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseSequence
	}
}

impl<'a> Sq<'a> {
	pub(crate) fn new(ns: NamespaceId, db: DatabaseId, sq: &'a str) -> Self {
		Self {
			__: b'/', // /
			_a: b'*', // *
			ns,
			_b: b'*', // *
			db,
			_c: b'*', // *
			_d: b's', // s
			_e: b'q', // q
			sq,
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
            let val = Sq::new(
            NamespaceId(1),
            DatabaseId(2),
            "test",
        );
		let enc = Sq::encode(&val).unwrap();
		assert_eq!(enc, b"/*1\0*2\0*sqtest\0");
		let dec = Sq::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn prefix() {
		let val = super::prefix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*1\0*2\0*sq\0");
	}

	#[test]
	fn suffix() {
		let val = super::suffix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*1\0*2\0*sq\xff");
	}
}
