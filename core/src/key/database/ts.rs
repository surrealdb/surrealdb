//! Stores database timestamps
use crate::key::category::Category;
use crate::key::key_req::KeyRequirements;
use derive::Key;
use serde::{Deserialize, Serialize};

// Ts stands for Database Timestamps that corresponds to Versionstamps.
// Each Ts key is suffixed by a timestamp.
// The value is the versionstamp that corresponds to the timestamp.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Ts<'a> {
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

pub fn new<'a>(ns: &'a str, db: &'a str, ts: u64) -> Ts<'a> {
	Ts::new(ns, db, ts)
}

/// Returns the prefix for the whole database timestamps
pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = crate::key::database::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b't', b's']);
	k
}

/// Returns the prefix for the whole database timestamps
pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = prefix(ns, db);
	k.extend_from_slice(&[0xff]);
	k
}

impl KeyRequirements for Ts<'_> {
	fn key_category(&self) -> Category {
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
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ts::new(
			"test",
			"test",
			123,
		);
		let enc = Ts::encode(&val).unwrap();
		let dec = Ts::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
