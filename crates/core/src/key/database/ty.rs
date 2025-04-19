//! Stores a DEFINE TYPE definition
use crate::err::Error;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{impl_key, KeyEncode};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Ty<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub ty: &'a str,
}
impl_key!(Ty<'a>);

pub fn new<'a>(ns: &'a str, db: &'a str, ty: &'a str) -> Ty<'a> {
	Ty::new(ns, db, ty)
}

pub fn prefix(ns: &str, db: &str) -> Result<Vec<u8>, Error> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!ty\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str) -> Result<Vec<u8>, Error> {
	let mut k = super::all::new(ns, db).encode()?;
	k.extend_from_slice(b"!ty\xff");
	Ok(k)
}

impl Categorise for Ty<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseType
	}
}

impl<'a> Ty<'a> {
	pub fn new(ns: &'a str, db: &'a str, ty: &'a str) -> Self {
		Self {
			__: b'/', // /
			_a: b'*', // *
			ns,
			_b: b'*', // *
			db,
			_c: b'!', // !
			_d: b't', // t
			_e: b'y', // y
			ty,
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
            let val = Ty::new(
            "ns",
            "db",
            "test",
        );
		let enc = Ty::encode(&val).unwrap();
		assert_eq!(enc, b"/*ns\0*db\0!tytest\0");
		let dec = Ty::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn prefix() {
		let val = super::prefix("namespace", "database").unwrap();
		assert_eq!(val, b"/*namespace\0*database\0!ty\0");
	}

	#[test]
	fn suffix() {
		let val = super::suffix("namespace", "database").unwrap();
		assert_eq!(val, b"/*namespace\0*database\0!ty\xff");
	}
}
