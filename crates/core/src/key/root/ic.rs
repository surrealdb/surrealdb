//! List of index that require compaction.
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Ic<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	ns: &'a str,
	db: &'a str,
	tb: &'a str,
	ix: &'a str,
	#[serde(with = "uuid::serde::compact")]
	pub nid: Uuid,
	#[serde(with = "uuid::serde::compact")]
	pub uid: Uuid,
}
impl_key!(Ic<'a>);

impl Categorise for Ic<'_> {
	fn categorise(&self) -> Category {
		Category::IndexCompaction
	}
}

impl<'a> Ic<'a> {
	pub(crate) fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		nid: Uuid,
		uid: Uuid,
	) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'i',
			_c: b'c',
			ns,
			db,
			tb,
			ix,
			nid,
			uid,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::{KeyDecode, KeyEncode};

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ic::new("testns", "testdb", "testtb", "testix", Uuid::from_u128(1), Uuid::from_u128(2));
		let enc = Ic::encode(&val).unwrap();
		assert_eq!(enc, b"/!ic\0\x01");
		let dec = Ic::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
