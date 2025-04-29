//! Stores a DEFINE ANALYZER config definition
use crate::err::Error;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{impl_key, KeyEncode};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Az<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub az: &'a str,
}
impl_key!(Az<'a>);

pub fn new<'a>(ns: &'a str, db: &'a str, az: &'a str) -> Az<'a> {
	Az::new(ns, db, az)
}

pub fn prefix(ns: &str, db: &str) -> Result<Vec<u8>, Error> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!az\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str) -> Result<Vec<u8>, Error> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!az\xff");
	Ok(k)
}

impl Categorise for Az<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseAnalyzer
	}
}

impl<'a> Az<'a> {
	pub fn new(ns: &'a str, db: &'a str, az: &'a str) -> Self {
		Self {
			__: b'/', // /
			_a: b'*', // *
			ns,
			_b: b'*', // *
			db,
			_c: b'!', // !
			_d: b'a', // a
			_e: b'z', // z
			az,
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
            let val = Az::new(
            "ns",
            "db",
            "test",
        );
		let enc = Az::encode(&val).unwrap();
		assert_eq!(enc, b"/*ns\0*db\0!aztest\0");
		let dec = Az::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn prefix() {
		let val = super::prefix("namespace", "database").unwrap();
		assert_eq!(val, b"/*namespace\0*database\0!az\0");
	}

	#[test]
	fn suffix() {
		let val = super::suffix("namespace", "database").unwrap();
		assert_eq!(val, b"/*namespace\0*database\0!az\xff");
	}
}
