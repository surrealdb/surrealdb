//! Stores a grant associated with an access method
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::expr::statements::access::AccessGrantStore;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Gr<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub ac: &'a str,
	_d: u8,
	_e: u8,
	_f: u8,
	pub gr: &'a str,
}

impl KVKey for Gr<'_> {
	type ValueType = AccessGrantStore;
}

pub fn new<'a>(ns: &'a str, db: &'a str, ac: &'a str, gr: &'a str) -> Gr<'a> {
	Gr::new(ns, db, ac, gr)
}

pub fn prefix(ns: &str, db: &str, ac: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, ac).encode_key()?;
	k.extend_from_slice(b"!gr\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str, ac: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db, ac).encode_key()?;
	k.extend_from_slice(b"!gr\xff");
	Ok(k)
}

impl Categorise for Gr<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseAccessGrant
	}
}

impl<'a> Gr<'a> {
	pub fn new(ns: &'a str, db: &'a str, ac: &'a str, gr: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'&',
			ac,
			_d: b'!',
			_e: b'g',
			_f: b'r',
			gr,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Gr::new(
			"testns",
			"testdb",
			"testac",
			"testgr",
		);
		let enc = Gr::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0&testac\0!grtestgr\0");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns", "testdb", "testac").unwrap();
		assert_eq!(val, b"/*testns\0*testdb\0&testac\0!gr\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns", "testdb", "testac").unwrap();
		assert_eq!(val, b"/*testns\0*testdb\0&testac\0!gr\xff");
	}
}
