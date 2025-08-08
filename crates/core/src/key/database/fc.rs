//! Stores a DEFINE FUNCTION config definition
use crate::expr::statements::define::DefineFunctionStatement;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Fc<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub fc: &'a str,
}

impl KVKey for Fc<'_> {
	type ValueType = DefineFunctionStatement;
}

pub fn new<'a>(ns: &'a str, db: &'a str, fc: &'a str) -> Fc<'a> {
	Fc::new(ns, db, fc)
}

pub fn prefix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!fn\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!fn\xff");
	Ok(k)
}

impl Categorise for Fc<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseFunction
	}
}

impl<'a> Fc<'a> {
	pub fn new(ns: &'a str, db: &'a str, fc: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'f',
			_e: b'n',
			fc,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Fc::new(
			"testns",
			"testdb",
			"testfc",
		);
		let enc = Fc::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\x00*testdb\x00!fntestfc\x00");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns", "testdb").unwrap();
		assert_eq!(val, b"/*testns\0*testdb\0!fn\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns", "testdb").unwrap();
		assert_eq!(val, b"/*testns\0*testdb\0!fn\xff");
	}
}
