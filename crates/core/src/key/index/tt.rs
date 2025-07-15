//! Stores the term/document frequency and offsets

use crate::idx::docids::DocId;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{KeyEncode, impl_key};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Tt<'a> {
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
	pub term: &'a str,
	pub doc_id: DocId,
	#[serde(with = "uuid::serde::compact")]
	pub nid: Uuid,
	#[serde(with = "uuid::serde::compact")]
	pub uid: Uuid,
	pub add: bool,
}

impl_key!(Tt<'a>);

impl Categorise for Tt<'_> {
	fn categorise(&self) -> Category {
		Category::IndexTermDocuments
	}
}

impl<'a> Tt<'a> {
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		term: &'a str,
		doc_id: DocId,
		nid: Uuid,
		uid: Uuid,
		add: bool,
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
			_f: b't',
			_g: b't',
			term,
			doc_id,
			nid,
			uid,
			add,
		}
	}

	pub(crate) fn range(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		term: &'a str,
	) -> Result<(Vec<u8>, Vec<u8>)> {
		let prefix = TtPrefix::new(ns, db, tb, ix, term);
		let mut beg = prefix.encode()?;
		beg.extend([0; 41]);
		let mut end = prefix.encode()?;
		end.extend([255; 41]);
		Ok((beg, end))
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
struct TtPrefix<'a> {
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
	pub term: &'a str,
}
impl_key!(TtPrefix<'a>);

impl<'a> TtPrefix<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, term: &'a str) -> Self {
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
			_f: b't',
			_g: b't',
			term,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KeyDecode;

	#[test]
	fn key() {
		let val = Tt::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			"term",
			129,
			Uuid::from_u128(1),
			Uuid::from_u128(2),
			true,
		);
		let enc = Tt::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!ttterm\0\0\0\0\0\0\0\0\x81\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02\x01");
		let dec = Tt::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn range() {
		let (beg, end) = Tt::range("testns", "testdb", "testtb", "testix", "term").unwrap();
		assert_eq!(beg, b"/*testns\0*testdb\0*testtb\0+testix\0!ttterm\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
		assert_eq!(
			end,
			b"/*testns\0*testdb\0*testtb\0+testix\0!ttterm\0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
		);
	}
}
