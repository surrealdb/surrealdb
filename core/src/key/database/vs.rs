//! Stores database versionstamps
use crate::key::category::Categorise;
use crate::key::category::Category;
use derive::Key;
use serde::{Deserialize, Serialize};

// Vs stands for Database Versionstamp
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Vs<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
}

#[allow(unused)]
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
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Vs::new(
			"test",
			"test",
		);
		let enc = Vs::encode(&val).unwrap();
		let dec = Vs::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
