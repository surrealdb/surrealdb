//! Stores a DEFINE CONFIG definition
use crate::expr::statements::define::config::ConfigStore;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Cg<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub ty: &'a str,
}

impl KVKey for Cg<'_> {
	type ValueType = ConfigStore;
}

pub fn new<'a>(ns: &'a str, db: &'a str, ty: &'a str) -> Cg<'a> {
	Cg::new(ns, db, ty)
}

pub fn prefix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(&[b'!', b'c', b'g', 0x00]);
	Ok(k)
}

pub fn suffix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(&[b'!', b'c', b'g', 0xff]);
	Ok(k)
}

impl Categorise for Cg<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseConfig
	}
}

impl<'a> Cg<'a> {
	pub fn new(ns: &'a str, db: &'a str, ty: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'c',
			_e: b'g',
			ty,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Cg::new(
			"testns",
			"testdb",
			"testty",
		);
		let enc = Cg::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\x00*testdb\x00!cgtestty\x00");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns", "testdb").unwrap();
		assert_eq!(val, b"/*testns\0*testdb\0!cg\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns", "testdb").unwrap();
		assert_eq!(val, b"/*testns\0*testdb\0!cg\xff");
	}
}
