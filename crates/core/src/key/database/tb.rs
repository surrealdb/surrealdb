//! Stores a DEFINE TABLE config definition
use crate::err::Error;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{impl_key, KeyEncode};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Tb<'a> {
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
impl_key!(Tb<'a>);

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str) -> Tb<'a> {
	Tb::new(ns, db, tb)
}

pub fn prefix(ns: &str, db: &str) -> Result<Vec<u8>, Error> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!tb\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str) -> Result<Vec<u8>, Error> {
	let mut k = super::all::new(ns, db).encode()?;
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
	use crate::kvs::KeyDecode;
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Tb::new(
			"testns",
			"testdb",
			"testtb",
		);
		let enc = Tb::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0!tbtesttb\0");

		let dec = Tb::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
