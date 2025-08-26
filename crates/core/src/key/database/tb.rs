//! Stores a DEFINE TABLE config definition

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId, TableDefinition};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct TableKey<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
	pub tb: &'a str,
}

impl KVKey for TableKey<'_> {
	type ValueType = TableDefinition;
}

pub fn new(ns: NamespaceId, db: DatabaseId, tb: &str) -> TableKey<'_> {
	TableKey::new(ns, db, tb)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!tb\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!tb\xff");
	Ok(k)
}

impl Categorise for TableKey<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseTable
	}
}

impl<'a> TableKey<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b't',
			_e: b'b',
			tb,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = TableKey::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
		);
		let enc = TableKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!tbtesttb\0");
	}
}
