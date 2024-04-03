//! Stores the key prefix for all keys under a scope
use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Scope<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub sc: &'a str,
}

pub fn new<'a>(ns: &'a str, db: &'a str, sc: &'a str) -> Scope<'a> {
	Scope::new(ns, db, sc)
}

impl KeyRequirements for Scope<'_> {
	fn key_category(&self) -> KeyCategory {
		KeyCategory::ScopeRoot
	}
}

impl<'a> Scope<'a> {
	pub fn new(ns: &'a str, db: &'a str, sc: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: super::CHAR,
			sc,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Scope::new(
			"testns",
			"testdb",
			"testsc",
		);
		let enc = Scope::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0\xb1testsc\0");

		let dec = Scope::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
