//! Stores a DEFINE FIELD config definition
use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{self, DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::{KVKey, impl_kv_key_storekey};
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Fd<'a> {
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
	pub fd: Cow<'a, str>,
}

impl_kv_key_storekey!(Fd<'_> => catalog::FieldDefinition);

pub fn new<'a>(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, fd: &'a str) -> Fd<'a> {
	Fd::new(ns, db, tb, fd)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId, tb: &TableName) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"!fd\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId, tb: &TableName) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"!fd\xff");
	Ok(k)
}

impl Categorise for Fd<'_> {
	fn categorise(&self) -> Category {
		Category::TableField
	}
}

impl<'a> Fd<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, fd: &'a str) -> Self {
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
			_f: b'd',
			fd: Cow::Borrowed(fd),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = Fd::new(NamespaceId(1), DatabaseId(2), &tb, "testfd");
		let enc = Fd::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fdtestfd\0");
	}

	#[test]
	fn test_prefix() {
		let tb = TableName::from("testtb");
		let val = super::prefix(NamespaceId(1), DatabaseId(2), &tb).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fd\0");
	}

	#[test]
	fn test_suffix() {
		let tb = TableName::from("testtb");
		let val = super::suffix(NamespaceId(1), DatabaseId(2), &tb).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fd\xff");
	}
}
