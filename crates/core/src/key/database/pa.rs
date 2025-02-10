//! Stores a DEFINE PARAM config definition
use crate::err::Error;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{impl_key, KeyEncode};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Pa<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub pa: &'a str,
}
impl_key!(Pa<'a>);

pub fn new<'a>(ns: &'a str, db: &'a str, pa: &'a str) -> Pa<'a> {
	Pa::new(ns, db, pa)
}

pub fn prefix(ns: &str, db: &str) -> Result<Vec<u8>, Error> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!pa\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str) -> Result<Vec<u8>, Error> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!pa\xff");
	Ok(k)
}

impl Categorise for Pa<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseParameter
	}
}

impl<'a> Pa<'a> {
	pub fn new(ns: &'a str, db: &'a str, pa: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'p',
			_e: b'a',
			pa,
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
		let val = Pa::new(
			"testns",
			"testdb",
			"testpa",
		);
		let enc = Pa::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0!patestpa\0");

		let dec = Pa::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
