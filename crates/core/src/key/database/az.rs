//! Stores a DEFINE ANALYZER config definition
use crate::expr::statements::define::DefineAnalyzerStatement;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Az<'a> {
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

impl KVKey for Az<'_> {
	type ValueType = DefineAnalyzerStatement;
}

pub fn new<'a>(ns: &'a str, db: &'a str, az: &'a str) -> Az<'a> {
	Az::new(ns, db, az)
}

pub fn prefix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!az\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
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
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
            let val = Az::new(
            "ns",
            "db",
            "test",
        );
		let enc = Az::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*ns\0*db\0!aztest\0");
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
