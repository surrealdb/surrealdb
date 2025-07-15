//! Stores the term/document frequency and offsets for a document

use crate::idx::docids::DocId;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{KeyEncode, impl_key};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Dc<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	_f: u8,
	_g: u8,
	pub doc_id: DocId,
	#[serde(with = "uuid::serde::compact")]
	pub nid: Uuid,
	#[serde(with = "uuid::serde::compact")]
	pub uid: Uuid,
}
impl_key!(Dc<'a>);

impl Categorise for Dc<'_> {
	fn categorise(&self) -> Category {
		Category::IndexFullTextDocCountAndLength
	}
}

impl<'a> Dc<'a> {
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		doc_id: DocId,
		nid: Uuid,
		uid: Uuid,
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
			_f: b'd',
			_g: b'c',
			doc_id,
			nid,
			uid,
		}
	}

	pub(crate) fn new_root(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str) -> Result<Vec<u8>> {
		DcPrefix::new(ns, db, tb, ix).encode()
	}

	pub(crate) fn range(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
	) -> Result<(Vec<u8>, Vec<u8>)> {
		let prefix = DcPrefix::new(ns, db, tb, ix);
		let mut beg = prefix.encode()?;
		beg.extend_from_slice(b"\0");
		let mut end = prefix.encode()?;
		end.extend_from_slice(b"\xff\xff\xff\xff\xff\xff\xff\xff");
		Ok((beg, end))
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
struct DcPrefix<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	_f: u8,
	_g: u8,
}
impl_key!(DcPrefix<'a>);

impl<'a> DcPrefix<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str) -> Self {
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
			_f: b'd',
			_g: b'c',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KeyDecode;

	#[test]
	fn key_with_ids() {
		let val = Dc::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			129,
			Uuid::from_u128(1),
			Uuid::from_u128(2),
		);
		let enc = Dc::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!dc\x01\0\0\0\0\0\0\0\x81\0\0\0\0\0\0\0\x10\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x10\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02");
		let dec = Dc::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn key_root() {
		let enc = Dc::new_root("testns", "testdb", "testtb", "testix").unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!dc\0");
		let _ = DcPrefix::decode(&enc).unwrap();
	}

	#[test]
	fn range() {
		let (beg, end) = Dc::range("testns", "testdb", "testtb", "testix").unwrap();
		assert_eq!(beg, b"/*testns\0*testdb\0*testtb\0+testix\0!dc\0");
		assert_eq!(
			end,
			b"/*testns\0*testdb\0*testtb\0+testix\0!dc\x01\xff\xff\xff\xff\xff\xff\xff\xff"
		);
	}
}
