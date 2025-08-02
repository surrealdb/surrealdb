//! Stores a DEFINE TABLE config definition
use crate::expr::statements::DefineTableStatement;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Tb<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub tb: &'a str,
}

impl KVKey for Tb<'_> {
	type ValueType = DefineTableStatement;
}

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str) -> Tb<'a> {
	Tb::new(ns, db, tb)
}

pub fn prefix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!tb\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!tb\xff");
	Ok(k)
}

impl Categorise for Tb<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseTable
	}
}

impl<'a> Tb<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str) -> Self {
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
		let val = Tb::new(
			"testns",
			"testdb",
			"testtb",
		);
		let enc = Tb::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0!tbtesttb\0");
	}
}
