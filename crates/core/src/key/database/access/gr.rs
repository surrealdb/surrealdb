//! Stores a grant associated with an access method
use crate::err::Error;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{impl_key, KeyEncode};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Gr<'a> {
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
impl_key!(Gr<'a>);

pub fn new<'a>(ns: &'a str, db: &'a str, ac: &'a str, gr: &'a str) -> Gr<'a> {
	Gr::new(ns, db, ac, gr)
}

pub fn prefix(ns: &str, db: &str, ac: &str) -> Result<Vec<u8>, Error> {
	let mut k = super::all::new(ns, db, ac).encode()?;
	k.extend_from_slice(b"!gr\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str, ac: &str) -> Result<Vec<u8>, Error> {
	let mut k = super::all::new(ns, db, ac).encode()?;
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
	use crate::kvs::KeyDecode;
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Gr::new(
			"testns",
			"testdb",
			"testac",
			"testgr",
		);
		let enc = Gr::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0&testac\0!grtestgr\0");

		let dec = Gr::decode(&enc).unwrap();
		assert_eq!(val, dec);
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
