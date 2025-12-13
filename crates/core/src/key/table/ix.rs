//! Stores a DEFINE INDEX config definition
use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexDefinition, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::{KVKey, impl_kv_key_storekey};
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Encode, BorrowDecode)]
pub(crate) struct IndexNameLookupKey<'key> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'key, str>,
	_d: u8,
	_e: u8,
	_f: u8,
	pub ix: IndexId,
}

impl_kv_key_storekey!(IndexNameLookupKey<'_> => String);

impl<'key> IndexNameLookupKey<'key> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'key str, ix: IndexId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'!',
			_e: b'i',
			_f: b'l',
			ix,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Encode, BorrowDecode)]
pub(crate) struct IndexDefinitionKey<'key> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'key, str>,
	_d: u8,
	_e: u8,
	_f: u8,
	pub ix: Cow<'key, str>,
}

impl_kv_key_storekey!(IndexDefinitionKey<'_> => IndexDefinition);

pub fn new<'key>(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &'key str,
	ix: &'key str,
) -> IndexDefinitionKey<'key> {
	IndexDefinitionKey::new(ns, db, tb, ix)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId, tb: &TableName) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"!ix\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId, tb: &TableName) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"!ix\xff");
	Ok(k)
}

impl Categorise for IndexDefinitionKey<'_> {
	fn categorise(&self) -> Category {
		Category::IndexDefinition
	}
}

impl<'key> IndexDefinitionKey<'key> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'key str, ix: &'key str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'!',
			_e: b'i',
			_f: b'x',
			ix: Cow::Borrowed(ix),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = IndexDefinitionKey::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testix",
		);
		let enc = IndexDefinitionKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\0\0\0\x01*\0\0\0\x02*testtb\0!ixtestix\0");
	}
}
