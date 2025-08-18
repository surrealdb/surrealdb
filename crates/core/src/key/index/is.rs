//! Index Sequence State Key Structure
//!
//! This module defines the `Is` key structure used to store distributed
//! sequence states for full-text search document ID generation. The key enables
//! concurrent indexing by maintaining a sequence state per node in a
//! distributed system.
//!
//! # Purpose
//!
//! The `Is` key stores the state of distributed sequences used to provide
//! unique numeric IDs to documents during full-text indexing operations. This
//! allows multiple nodes to concurrently index documents while maintaining
//! unique document identifiers.
//!
//! # Key Structure
//!
//! The key follows the pattern: `/*{ns}*{db}*{tb}+{ix}!ib{nid}`
//!
//! Where:
//! - `ns`: Namespace identifier
//! - `db`: Database identifier
//! - `tb`: Table identifier
//! - `ix`: Index identifier
//! - `nid`: Node UUID (16 bytes, compact serialized)
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use crate::kvs::sequences::SequenceState;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Is<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	_f: u8,
	_g: u8,
	#[serde(with = "uuid::serde::compact")]
	pub nid: Uuid,
}

impl KVKey for Is<'_> {
	type ValueType = SequenceState;
}

impl Categorise for Is<'_> {
	fn categorise(&self) -> Category {
		Category::IndexFullTextDocIdsSequenceState
	}
}

impl<'a> Is<'a> {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		ix: &'a str,
		nid: Uuid,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'+',
			ix,
			_e: b'!',
			_f: b'i',
			_g: b's',
			nid,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		let val = Is::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testix",
			Uuid::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
		);
		let enc = Is::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!is\0\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f");
	}
}
