//! Stores a DEFINE INDEX config definition
use crate::expr::DefineIndexStatement;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct Ix<'a> {
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
	pub ix: &'a str,
}

impl KVKey for Ix<'_> {
	type ValueType = DefineIndexStatement;
}

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str) -> Ix<'a> {
	Ix::new(ns, db, tb, ix)
}

pub fn prefix(ns: &str, db: &str, tb: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"!ix\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str, tb: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, tb).encode_key()?;
	k.extend_from_slice(b"!ix\xff");
	Ok(k)
}

impl Categorise for Ix<'_> {
	fn categorise(&self) -> Category {
		Category::IndexDefinition
	}
}

impl<'a> Ix<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'!',
			_e: b'i',
			_f: b'x',
			ix,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Ix::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
		);
		let enc = Ix::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0!ixtestix\0");
	}
}
