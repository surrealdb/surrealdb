//! Stores a DEFINE PARAM config definition
use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId, ParamDefinition};
use crate::key::category::{Categorise, Category};
use crate::kvs::{KVKey, impl_kv_key_storekey};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Pa<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
	pub pa: Cow<'a, str>,
}

impl_kv_key_storekey!(Pa<'_> => ParamDefinition);

pub fn new(ns: NamespaceId, db: DatabaseId, pa: &str) -> Pa<'_> {
	Pa::new(ns, db, pa)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!pa\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
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
			pa: Cow::Borrowed(pa),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		let val = Pa::new(NamespaceId(1), DatabaseId(2), "testpa");
		let enc = Pa::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!patestpa\0");
	}
}
