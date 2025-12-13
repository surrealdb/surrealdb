//! Stores a DEFINE TABLE AS config definition
use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId, TableDefinition};
use crate::key::category::{Categorise, Category};
use crate::kvs::{KVKey, impl_kv_key_storekey};
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Ft<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	_e: u8,
	_f: u8,
	pub ft: Cow<'a, str>,
}

impl_kv_key_storekey!(Ft<'_> => TableDefinition);

pub fn new<'a>(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, ft: &'a str) -> Ft<'a> {
	Ft::new(ns, db, tb, ft)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId, tb: &TableName) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"!ft\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId, tb: &TableName) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"!ft\xff");
	Ok(k)
}

impl Categorise for Ft<'_> {
	fn categorise(&self) -> Category {
		Category::TableView
	}
}

impl<'a> Ft<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, ft: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'!',
			_e: b'f',
			_f: b't',
			ft: Cow::Borrowed(ft),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Ft::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testft",
		);
		let enc = Ft::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fttestft\0");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(NamespaceId(1), DatabaseId(2), "testtb").unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!ft\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(NamespaceId(1), DatabaseId(2), "testtb").unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!ft\xff");
	}
}
