//! Stores a grant associated with an access method
use crate::expr::statements::AccessGrant;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::KVKey;
use crate::kvs::{KeyEncode, impl_key};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) struct Gr<'a> {
	__: u8,
	_a: u8,
	pub ac: &'a str,
	_b: u8,
	_c: u8,
	_d: u8,
	pub gr: &'a str,
}
impl_key!(Gr<'a>);

impl KVKey for Gr<'_> {
	type ValueType = AccessGrant;
}

pub fn new<'a>(ac: &'a str, gr: &'a str) -> Gr<'a> {
	Gr::new(ac, gr)
}

pub fn prefix(ac: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ac).encode()?;
	k.extend_from_slice(b"!gr\x00");
	Ok(k)
}

pub fn suffix(ac: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ac).encode()?;
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
	use crate::kvs::KeyDecode;
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Gr::new(
			"testac",
			"testgr",
		);
		let enc = Gr::encode(&val).unwrap();
		assert_eq!(enc, b"/&testac\0!grtestgr\0");

		let dec = Gr::decode(&enc).unwrap();
		assert_eq!(val, dec);
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
