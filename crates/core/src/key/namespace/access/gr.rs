//! Stores a grant associated with an access method
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::catalog;
use crate::catalog::NamespaceId;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Gr<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub ac: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub gr: &'a str,
}

impl KVKey for Gr<'_> {
	type ValueType = catalog::AccessGrant;
}

pub fn new<'a>(ns: NamespaceId, ac: &'a str, gr: &'a str) -> Gr<'a> {
	Gr::new(ns, ac, gr)
}

pub fn prefix(ns: NamespaceId, ac: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, ac).encode_key()?;
	k.extend_from_slice(b"!gr\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, ac: &str) -> Result<Vec<u8>> {
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
	pub fn new(ns: NamespaceId, ac: &'a str, gr: &'a str) -> Self {
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
			NamespaceId(1),
			"testac",
			"testgr",
		);
		let enc = Gr::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01&testac\0!grtestgr\0");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(NamespaceId(1), "testac").unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01&testac\0!gr\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(NamespaceId(1), "testac").unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01&testac\0!gr\xff");
	}
}
