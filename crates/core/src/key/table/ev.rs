//! Stores a DEFINE EVENT config definition
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, EventDefinition, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Ev<'a> {
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
	pub ev: &'a str,
}

impl KVKey for Ev<'_> {
	type ValueType = EventDefinition;
}

pub fn new<'a>(ns: NamespaceId, db: DatabaseId, tb: &'a str, ev: &'a str) -> Ev<'a> {
	Ev::new(ns, db, tb, ev)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId, tb: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"!ev\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId, tb: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"!ev\xff");
	Ok(k)
}

impl Categorise for Ev<'_> {
	fn categorise(&self) -> Category {
		Category::TableEvent
	}
}

impl<'a> Ev<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ev: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'!',
			_e: b'e',
			_f: b'v',
			ev,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Ev::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testev",
		);
		let enc = Ev::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!evtestev\0");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(NamespaceId(1), DatabaseId(2), "testtb").unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!ev\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(NamespaceId(1), DatabaseId(2), "testtb").unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!ev\xff");
	}
}
