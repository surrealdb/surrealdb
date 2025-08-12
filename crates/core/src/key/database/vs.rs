//! Stores database versionstamps
use serde::{Deserialize, Serialize};

use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use crate::vs::VersionStamp;

// Vs stands for Database Versionstamp
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Vs<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
}

impl KVKey for Vs<'_> {
	type ValueType = VersionStamp;
}

pub fn new<'a>(ns: &'a str, db: &'a str) -> Vs<'a> {
	Vs::new(ns, db)
}

impl Categorise for Vs<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseVersionstamp
	}
}

impl<'a> Vs<'a> {
	pub fn new(ns: &'a str, db: &'a str) -> Self {
		Vs {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'v',
			_e: b's',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Vs::new(
			"test",
			"test",
		);
		let enc = Vs::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*test\0*test\0!vs");
	}
}
