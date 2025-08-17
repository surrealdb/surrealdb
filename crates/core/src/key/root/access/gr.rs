//! Stores a grant associated with an access method
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::catalog;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Gr<'a> {
	__: u8,
	_a: u8,
	pub ac: &'a str,
	_b: u8,
	_c: u8,
	_d: u8,
	pub gr: &'a str,
}

impl KVKey for Gr<'_> {
	type ValueType = catalog::AccessGrant;
}

pub fn new<'a>(ac: &'a str, gr: &'a str) -> Gr<'a> {
	Gr::new(ac, gr)
}

pub fn prefix(ac: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ac).encode_key()?;
	k.extend_from_slice(b"!gr\x00");
	Ok(k)
}

pub fn suffix(ac: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ac).encode_key()?;
	k.extend_from_slice(b"!gr\xff");
	Ok(k)
}

impl Categorise for Gr<'_> {
	fn categorise(&self) -> Category {
		Category::AccessGrant
	}
}

impl<'a> Gr<'a> {
	pub fn new(ac: &'a str, gr: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'&',
			ac,
			_b: b'!',
			_c: b'g',
			_d: b'r',
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
			"testac",
			"testgr",
		);
		let enc = Gr::encode_key(&val).unwrap();
		assert_eq!(enc, b"/&testac\0!grtestgr\0");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testac").unwrap();
		assert_eq!(val, b"/&testac\0!gr\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testac").unwrap();
		assert_eq!(val, b"/&testac\0!gr\xff");
	}
}
