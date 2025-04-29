//! Stores a DEFINE BUCKET definition
use serde::{Deserialize, Serialize};

use crate::err::Error;
use crate::key::category::{Categorise, Category};
use crate::kvs::{impl_key, KeyEncode};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Bu<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub bu: &'a str,
}
impl_key!(Bu<'a>);

pub fn new<'a>(ns: &'a str, db: &'a str, bu: &'a str) -> Bu<'a> {
	Bu::new(ns, db, bu)
}

pub fn prefix(ns: &str, db: &str) -> Result<Vec<u8>, Error> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!bu\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str) -> Result<Vec<u8>, Error> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!bu\xff");
	Ok(k)
}

impl Categorise for Bu<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseBucket
	}
}

impl<'a> Bu<'a> {
	pub fn new(ns: &'a str, db: &'a str, bu: &'a str) -> Self {
		Self {
			__: b'/', // /
			_a: b'*', // *
			ns,
			_b: b'*', // *
			db,
			_c: b'!', // !
			_d: b'b', // b
			_e: b'u', // u
			bu,
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
            let val = Bu::new(
            "ns",
            "db",
            "test",
        );
		let enc = Bu::encode(&val).unwrap();
		assert_eq!(enc, b"/*ns\0*db\0!butest\0");
		let dec = Bu::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn prefix() {
		let val = super::prefix("namespace", "database").unwrap();
		assert_eq!(val, b"/*namespace\0*database\0!bu\0");
	}

	#[test]
	fn suffix() {
		let val = super::suffix("namespace", "database").unwrap();
		assert_eq!(val, b"/*namespace\0*database\0!bu\xff");
	}
}
