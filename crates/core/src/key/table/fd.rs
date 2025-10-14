//! Stores a DEFINE FIELD config definition
use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{self, DatabaseId, NamespaceId};
use crate::kvs::{KVKey, impl_kv_key_storekey};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct FieldDefinitionKey<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
	_d: u8,
	_e: u8,
	_f: u8,
	pub fd: Cow<'a, str>,
}

impl_kv_key_storekey!(FieldDefinitionKey<'_> => catalog::FieldDefinition);

pub fn new<'a>(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &'a str,
	fd: &'a str,
) -> FieldDefinitionKey<'a> {
	FieldDefinitionKey::new(ns, db, tb, fd)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId, tb: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"!fd\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId, tb: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"!fd\xff");
	Ok(k)
}

impl<'a> FieldDefinitionKey<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, fd: &'a str) -> Self {
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
		#[rustfmt::skip]
		let val = FieldDefinitionKey::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testfd",
		);
		let enc = FieldDefinitionKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fdtestfd\0");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(NamespaceId(1), DatabaseId(2), "testtb").unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fd\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(NamespaceId(1), DatabaseId(2), "testtb").unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fd\xff");
	}
}
