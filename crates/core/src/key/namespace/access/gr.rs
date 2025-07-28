//! Stores a grant associated with an access method
use crate::expr::statements::AccessGrant;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::KVKey;

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Gr<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub ac: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub gr: &'a str,
}

impl KVKey for Gr<'_> {
	type ValueType = AccessGrant;
}

pub fn new<'a>(ns: &'a str, ac: &'a str, gr: &'a str) -> Gr<'a> {
	Gr::new(ns, ac, gr)
}

pub fn prefix(ns: &str, ac: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, ac).encode_key()?;
	k.extend_from_slice(b"!gr\x00");
	Ok(k)
}

pub fn suffix(ns: &str, ac: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, ac).encode_key()?;
	k.extend_from_slice(b"!gr\xff");
	Ok(k)
}

impl Categorise for Gr<'_> {
	fn categorise(&self) -> Category {
		Category::NamespaceAccessGrant
	}
}

impl<'a> Gr<'a> {
	pub fn new(ns: &'a str, ac: &'a str, gr: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'&',
			ac,
			_c: b'!',
			_d: b'g',
			_e: b'r',
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
			"testac",
			"testgr",
		);
		let enc = Gr::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0&testac\0!grtestgr\0");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns", "testac").unwrap();
		assert_eq!(val, b"/*testns\0&testac\0!gr\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns", "testac").unwrap();
		assert_eq!(val, b"/*testns\0&testac\0!gr\xff");
	}
}
