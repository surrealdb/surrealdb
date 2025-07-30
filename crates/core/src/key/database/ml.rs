//! Stores a DEFINE MODEL config definition
use crate::catalog::DatabaseId;
use crate::catalog::NamespaceId;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{KeyEncode, impl_key};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Ml<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
	pub ml: &'a str,
	pub vn: &'a str,
}
impl_key!(Ml<'a>);

pub fn new<'a>(ns: NamespaceId, db: DatabaseId, ml: &'a str, vn: &'a str) -> Ml<'a> {
	Ml::new(ns, db, ml, vn)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!ml\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!ml\xff");
	Ok(k)
}

impl Categorise for Ml<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseModel
	}
}

impl<'a> Ml<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, ml: &'a str, vn: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'm',
			_e: b'l',
			ml,
			vn,
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
		let val = Ml::new(
			NamespaceId(1),
			DatabaseId(2),
			"testml",
			"1.0.0",
		);
		let enc = Ml::encode(&val).unwrap();
		assert_eq!(enc, b"/*1\x00*2\x00!mltestml\x001.0.0\x00");
		let dec = Ml::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*1\0*2\0!ml\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*1\0*2\0!ml\xff");
	}
}
