//! Stores a DEFINE MODEL config definition
use crate::expr::statements::define::DefineModelStatement;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Ml<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub ml: &'a str,
	pub vn: &'a str,
}

impl KVKey for Ml<'_> {
	type ValueType = DefineModelStatement;
}

pub fn new<'a>(ns: &'a str, db: &'a str, ml: &'a str, vn: &'a str) -> Ml<'a> {
	Ml::new(ns, db, ml, vn)
}

pub fn prefix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!ml\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!ml\xff");
	Ok(k)
}

impl Categorise for Ml<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseModel
	}
}

impl<'a> Ml<'a> {
	pub fn new(ns: &'a str, db: &'a str, ml: &'a str, vn: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'm',
			_e: b'l',
			ml,
			vn,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Ml::new(
			"testns",
			"testdb",
			"testml",
			"1.0.0",
		);
		let enc = Ml::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\x00*testdb\x00!mltestml\x001.0.0\x00");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns", "testdb").unwrap();
		assert_eq!(val, b"/*testns\0*testdb\0!ml\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns", "testdb").unwrap();
		assert_eq!(val, b"/*testns\0*testdb\0!ml\xff");
	}
}
