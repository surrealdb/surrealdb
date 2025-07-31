//! Stores a DEFINE ACCESS ON NAMESPACE configuration
use crate::expr::statements::define::DefineAccessStatement;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Ac<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	_c: u8,
	_d: u8,
	pub ac: &'a str,
}

impl KVKey for Ac<'_> {
	type ValueType = DefineAccessStatement;
}

pub fn new<'a>(ns: &'a str, ac: &'a str) -> Ac<'a> {
	Ac::new(ns, ac)
}

pub fn prefix(ns: &str) -> Result<Vec<u8>> {
	let mut k = crate::key::namespace::all::new(ns).encode_key()?;
	k.extend_from_slice(b"!ac\x00");
	Ok(k)
}

pub fn suffix(ns: &str) -> Result<Vec<u8>> {
	let mut k = crate::key::namespace::all::new(ns).encode_key()?;
	k.extend_from_slice(b"!ac\xff");
	Ok(k)
}

impl Categorise for Ac<'_> {
	fn categorise(&self) -> Category {
		Category::NamespaceAccess
	}
}

impl<'a> Ac<'a> {
	pub fn new(ns: &'a str, ac: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'a',
			_d: b'c',
			ac,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Ac::new(
			"testns",
			"testac",
		);
		let enc = Ac::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0!actestac\0");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns").unwrap();
		assert_eq!(val, b"/*testns\0!ac\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns").unwrap();
		assert_eq!(val, b"/*testns\0!ac\xff");
	}
}
