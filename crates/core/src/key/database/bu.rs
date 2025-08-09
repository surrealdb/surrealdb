//! Stores a DEFINE BUCKET definition
use crate::expr::statements::define::BucketDefinition;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Bu<'a> {
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

impl KVKey for Bu<'_> {
	type ValueType = BucketDefinition;
}

pub fn new<'a>(ns: &'a str, db: &'a str, bu: &'a str) -> Bu<'a> {
	Bu::new(ns, db, bu)
}

pub fn prefix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!bu\x00");
	Ok(k)
}

pub fn suffix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
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
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
            let val = Bu::new(
            "ns",
            "db",
            "test",
        );
		let enc = Bu::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*ns\0*db\0!butest\0");
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
