//! Stores a DEFINE API definition
use crate::err::Error;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{impl_key, KeyEncode};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Ap<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub ap: &'a str,
}
impl_key!(Ap<'a>);

pub fn new<'a>(ns: &'a str, db: &'a str, ap: &'a str) -> Ap<'a> {
	Ap::new(ns, db, ap)
}

pub fn prefix(ns: &str, db: &str) -> Result<Vec<u8>, Error> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!ap\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str) -> Result<Vec<u8>, Error> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!ap\xff");
	Ok(k)
}

impl Categorise for Ap<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseApi
	}
}

impl<'a> Ap<'a> {
	pub fn new(ns: &'a str, db: &'a str, ap: &'a str) -> Self {
		Self {
			__: b'/', // /
			_a: b'*', // *
			ns,
			_b: b'*', // *
			db,
			_c: b'!', // !
			_d: b'a', // a
			_e: b'p', // p
			ap,
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
            let val = Ap::new(
            "ns",
            "db",
            "test",
        );
		let enc = Ap::encode(&val).unwrap();
		assert_eq!(enc, b"/*ns\0*db\0!aptest\0");
		let dec = Ap::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn prefix() {
		let val = super::prefix("namespace", "database").unwrap();
		assert_eq!(val, b"/*namespace\0*database\0!ap\0");
	}

	#[test]
	fn suffix() {
		let val = super::suffix("namespace", "database").unwrap();
		assert_eq!(val, b"/*namespace\0*database\0!ap\xff");
	}
}
