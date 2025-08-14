//! Stores database timestamps
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use crate::vs::VersionStamp;

// Ts stands for Database Timestamps that corresponds to Versionstamps.
// Each Ts key is suffixed by a timestamp.
// The value is the versionstamp that corresponds to the timestamp.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Ts<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub ts: u64,
}

impl KVKey for Ts<'_> {
	type ValueType = VersionStamp;
}

pub fn new<'a>(ns: &'a str, db: &'a str, ts: u64) -> Ts<'a> {
	Ts::new(ns, db, ts)
}

/// Returns the prefix for the whole database timestamps
pub fn prefix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!ts\x00");
	Ok(k)
}

/// Returns the prefix for the whole database timestamps
pub fn suffix(ns: &str, db: &str) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!ts\xff");
	Ok(k)
}

impl Categorise for Ts<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseTimestamp
	}
}

impl<'a> Ts<'a> {
	pub fn new(ns: &'a str, db: &'a str, ts: u64) -> Self {
		Ts {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b't',
			_e: b's',
			ts,
		}
	}

	pub fn decode_key(k: &[u8]) -> anyhow::Result<Ts<'_>> {
		Ok(storekey::deserialize(k)?)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Ts::new(
			"test",
			"test",
			123,
		);
		let enc = Ts::encode_key(&val).unwrap();
		assert_eq!(&enc, b"/*test\0*test\0!ts\x00\x00\x00\x00\x00\x00\x00\x7b");
	}
}
