//! Stores sequence states
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use crate::kvs::sequences::SequenceState;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct St<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
	pub sq: &'a str,
	_f: u8,
	_g: u8,
	_h: u8,
	#[serde(with = "uuid::serde::compact")]
	pub nid: Uuid,
}

impl KVKey for St<'_> {
	type ValueType = SequenceState;
}

impl Categorise for St<'_> {
	fn categorise(&self) -> Category {
		Category::SequenceState
	}
}

impl<'a> St<'a> {
	pub(crate) fn new(ns: NamespaceId, db: DatabaseId, sq: &'a str, nid: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b's',
			_e: b'q',
			sq,
			_f: b'!',
			_g: b's',
			_h: b't',
			nid,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		let val = St::new(
			NamespaceId(1),
			DatabaseId(2),
			"testsq",
			Uuid::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
		);
		let enc = St::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!sqtestsq\0!st\0\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f");
	}
}
