//! Unique index states

use std::borrow::Cow;
use std::ops::Range;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::{KVKey, impl_kv_key_storekey};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Iu<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
	_d: u8,
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
	pub uid: Option<(Uuid, Uuid)>,
	pub pos: bool,
	pub count: u32,
}

impl_kv_key_storekey!(Iu<'_> => Vec<u8>);

impl Categorise for Iu<'_> {
	fn categorise(&self) -> Category {
		Category::IndexCountState
	}
}

impl<'a> Iu<'a> {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		ix: IndexId,
		uid: Option<(Uuid, Uuid)>,
		pos: bool,
		count: u32,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'*',
			ix,
			_e: b'!',
			_f: b'i',
			_g: b'u',
			uid,
			pos,
			count,
		}
	}

	pub(crate) fn decode_key(k: &[u8]) -> Result<Iu<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}

	pub(crate) fn range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		ix: IndexId,
	) -> Result<Range<Vec<u8>>> {
		let mut beg = Prefix::new(ns, db, tb, ix).encode_key()?;
		let mut end = beg.clone();
		beg.push(0);
		end.push(0xff);
		Ok(beg..end)
	}
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Encode, BorrowDecode)]
struct Prefix<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
	_d: u8,
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
}

impl_kv_key_storekey!(Prefix<'_> => Vec<u8>);

impl<'a> Prefix<'a> {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: IndexId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'*',
			ix,
			_e: b'!',
			_f: b'i',
			_g: b'u',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = Iu::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			IndexId(3),
			Some((
				Uuid::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
				Uuid::from_bytes([15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
			)),
			true,
			65535,
		);
		let enc = Iu::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*\0\0\0\x03!iu\x03\0\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x0f\x0e\x0d\x0c\x0b\x0a\x09\x08\x07\x06\x05\x04\x03\x02\x01\0\x03\0\0\xff\xff", "key");
	}

	#[test]
	fn compacted_key() {
		let val = Iu::new(NamespaceId(1), DatabaseId(2), "testtb", IndexId(3), None, true, 65535);
		let enc = Iu::encode_key(&val).unwrap();
		assert_eq!(
			enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*\0\0\0\x03!iu\x02\x03\0\0\xff\xff",
			"compacted key"
		);
	}

	#[test]
	fn range() {
		let r = Iu::range(NamespaceId(1), DatabaseId(2), "testtb", IndexId(3)).unwrap();
		assert_eq!(
			r.start, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*\0\0\0\x03!iu\0",
			"start"
		);
		assert_eq!(r.end, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*\0\0\0\x03!iu\xff", "end");
	}
}
