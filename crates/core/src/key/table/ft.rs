//! Stores a DEFINE TABLE AS config definition
use crate::expr::statements::define::DefineTableStatement;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::KVKey;

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Ft<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	_e: u8,
	_f: u8,
	pub ft: &'a str,
}

impl KVKey for Ft<'_> {
	type ValueType = DefineTableStatement;
}

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str, ft: &'a str) -> Ft<'a> {
	Ft::new(ns, db, tb, ft)
}

pub fn prefix(ns: &str, db: &str, tb: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"!ft\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str, tb: &str) -> Result<Vec<u8>> {
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
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ft: &'a str) -> Self {
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
			_f: b't',
			ft,
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
			"testns",
			"testdb",
			"testtb",
			"testft",
		);
		let enc = Ft::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\x00*testdb\x00*testtb\x00!fttestft\x00");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns", "testdb", "testtb").unwrap();
		assert_eq!(val, b"/*testns\0*testdb\0*testtb\0!ft\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns", "testdb", "testtb").unwrap();
		assert_eq!(val, b"/*testns\0*testdb\0*testtb\0!ft\xff");
	}
}
