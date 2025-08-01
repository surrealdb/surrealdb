//! Stores a DEFINE FIELD config definition
use crate::expr::statements::DefineFieldStatement;
use crate::catalog::DatabaseId;
use crate::catalog::NamespaceId;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::KVKey;

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Fd<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	_e: u8,
	_f: u8,
	pub fd: &'a str,
}

impl KVKey for Fd<'_> {
	type ValueType = DefineFieldStatement;
}

pub fn new<'a>(ns: NamespaceId, db: DatabaseId, tb: &'a str, fd: &'a str) -> Fd<'a> {
	Fd::new(ns, db, tb, fd)
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

impl Categorise for Fd<'_> {
	fn categorise(&self) -> Category {
		Category::TableField
	}
}

impl<'a> Fd<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, fd: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'!',
			_e: b'f',
			_f: b'd',
			fd,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Fd::new(
			"testns",
			"testdb",
			"testtb",
			"testfd",
		);
		let enc = Fd::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\x00*testdb\x00*testtb\x00!fdtestfd\x00");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns", "testdb", "testtb").unwrap();
		assert_eq!(val, b"/*testns\0*testdb\0*testtb\0!fd\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns", "testdb", "testtb").unwrap();
		assert_eq!(val, b"/*testns\0*testdb\0*testtb\0!fd\xff");
	}
}
