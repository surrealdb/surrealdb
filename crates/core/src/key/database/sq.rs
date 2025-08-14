//! Stores a DEFINE SEQUENCE config definition
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::expr::statements::define::DefineSequenceStatement;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Sq<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub sq: &'a str,
}

impl KVKey for Sq<'_> {
	type ValueType = DefineSequenceStatement;
}

pub fn prefix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"*sq\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"*sq\xff");
	Ok(k)
}

impl Categorise for Sq<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseSequence
	}
}

impl<'a> Sq<'a> {
	pub(crate) fn new(ns: &'a str, db: &'a str, sq: &'a str) -> Self {
		Self {
			__: b'/', // /
			_a: b'*', // *
			ns,
			_b: b'*', // *
			db,
			_c: b'*', // *
			_d: b's', // s
			_e: b'q', // q
			sq,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
            let val = Sq::new(
            "ns",
            "db",
            "test",
        );
		let enc = Sq::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*ns\0*db\0*sqtest\0");
	}

	#[test]
	fn prefix() {
		let val = super::prefix("namespace", "database").unwrap();
		assert_eq!(val, b"/*namespace\0*database\0*sq\0");
	}

	#[test]
	fn suffix() {
		let val = super::suffix("namespace", "database").unwrap();
		assert_eq!(val, b"/*namespace\0*database\0*sq\xff");
	}
}
