//! Stores a DEFINE DATABASE config definition
use crate::err::Error;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{impl_key, KeyEncode};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Db<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	_c: u8,
	_d: u8,
	pub db: &'a str,
}
impl_key!(Db<'a>);

pub fn new<'a>(ns: &'a str, db: &'a str) -> Db<'a> {
	Db::new(ns, db)
}

pub fn prefix(ns: &str) -> Result<Vec<u8>, Error> {
	let mut k = super::all::new(ns).encode()?;
	k.extend_from_slice(b"!db\x00");
	Ok(k)
}

pub fn suffix(ns: &str) -> Result<Vec<u8>, Error> {
	let mut k = super::all::new(ns).encode()?;
	k.extend_from_slice(b"!db\xff");
	Ok(k)
}

impl Categorise for Db<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseAlias
	}
}

impl<'a> Db<'a> {
	pub fn new(ns: &'a str, db: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'd',
			_d: b'b',
			db,
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
		let val = Db::new(
			"testns",
			"testdb",
		);
		let enc = Db::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0!dbtestdb\0");

		let dec = Db::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix("testns").unwrap();
		assert_eq!(val, b"/*testns\0!db\0")
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix("testns").unwrap();
		assert_eq!(val, b"/*testns\0!db\xff")
	}
}
