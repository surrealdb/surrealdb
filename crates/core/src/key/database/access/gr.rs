//! Stores a grant associated with an access method

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{KeyEncode, impl_key};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Gr<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub ac: &'a str,
	_d: u8,
	_e: u8,
	_f: u8,
	pub gr: &'a str,
}
impl_key!(Gr<'a>);

pub fn new<'a>(ns: NamespaceId, db: DatabaseId, ac: &'a str, gr: &'a str) -> Gr<'a> {
	Gr::new(ns, db, ac, gr)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId, ac: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, ac).encode()?;
	k.extend_from_slice(b"!gr\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId, ac: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, ac).encode()?;
	k.extend_from_slice(b"!gr\xff");
	Ok(k)
}

impl Categorise for Gr<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseAccessGrant
	}
}

impl<'a> Gr<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, ac: &'a str, gr: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'&',
			ac,
			_d: b'!',
			_e: b'g',
			_f: b'r',
			gr,
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
		let val = Gr::new(
			NamespaceId(1),
			DatabaseId(2),
			"testac",
			"testgr",
		);
		let enc = Gr::encode(&val).unwrap();
		assert_eq!(enc, b"/*1\0*2\0&testac\0!grtestgr\0");

		let dec = Gr::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(NamespaceId(1), DatabaseId(2), "testac").unwrap();
		assert_eq!(val, b"/*1\0*2\0&testac\0!gr\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(NamespaceId(1), DatabaseId(2), "testac").unwrap();
		assert_eq!(val, b"/*1\0*2\0&testac\0!gr\xff");
	}
}
