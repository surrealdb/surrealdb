//! Stores a DEFINE PARAM config definition
use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{KeyEncode, impl_key};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Pa<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
	pub pa: &'a str,
}
impl_key!(Pa<'a>);

pub fn new<'a>(ns: NamespaceId, db: DatabaseId, pa: &'a str) -> Pa<'a> {
	Pa::new(ns, db, pa)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!pa\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!pa\xff");
	Ok(k)
}

impl Categorise for Pa<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseParameter
	}
}

impl<'a> Pa<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, pa: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'p',
			_e: b'a',
			pa,
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
		let val = Pa::new(
			NamespaceId(1),
			DatabaseId(2),
			"testpa",
		);
		let enc = Pa::encode(&val).unwrap();
		assert_eq!(enc, b"/*1\0*2\0!patestpa\0");

		let dec = Pa::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
