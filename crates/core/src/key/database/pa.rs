//! Stores a DEFINE PARAM config definition
use crate::expr::DefineParamStore;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Pa<'a> {
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

impl KVKey for Pa<'_> {
	type ValueType = DefineParamStore;
}

pub fn new<'a>(ns: &'a str, db: &'a str, pa: &'a str) -> Pa<'a> {
	Pa::new(ns, db, pa)
}

pub fn prefix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!pa\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
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
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Pa::new(
			"testns",
			"testdb",
			"testpa",
		);
		let enc = Pa::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0!patestpa\0");
	}
}
