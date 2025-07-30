//! Stores a DEFINE CONFIG definition
use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{KeyEncode, impl_key};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Cg<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
	pub ty: &'a str,
}
impl_key!(Cg<'a>);

pub fn new<'a>(ns: NamespaceId, db: DatabaseId, ty: &'a str) -> Cg<'a> {
	Cg::new(ns, db, ty)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(&[b'!', b'c', b'g', 0x00]);
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(&[b'!', b'c', b'g', 0xff]);
	Ok(k)
}

impl Categorise for Cg<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseConfig
	}
}

impl<'a> Cg<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, ty: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'c',
			_e: b'g',
			ty,
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
		let val = Cg::new(
			NamespaceId(1),
			DatabaseId(2),
			"testty",
		);
		let enc = Cg::encode(&val).unwrap();
		assert_eq!(enc, b"/*1\x00*2\x00!cgtestty\x00");
		let dec = Cg::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*1\0*2\0!cg\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*1\0*2\0!cg\xff");
	}
}
