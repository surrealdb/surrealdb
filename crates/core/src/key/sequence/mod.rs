//! Stores sequence states
pub mod ba;
pub mod st;

use std::borrow::Cow;
use std::ops::Range;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::kvs::{KVKey, impl_kv_key_storekey};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Prefix<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
	pub sq: Cow<'a, str>,
	_f: u8,
	_g: u8,
	_h: u8,
}

impl_kv_key_storekey!(Prefix<'_> => Vec<u8>);

impl<'a> Prefix<'a> {
	fn new(ns: NamespaceId, db: DatabaseId, sq: &'a str, g: u8, h: u8) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b's',
			_e: b'q',
			sq: Cow::Borrowed(sq),
			_f: b'!',
			_g: g,
			_h: h,
		}
	}

	pub(crate) fn new_range(
		ns: NamespaceId,
		db: DatabaseId,
		g: u8,
		h: u8,
		sq: &'a str,
	) -> Result<Range<Vec<u8>>> {
		let mut beg = Self::new(ns, db, sq, g, h).encode_key()?;
		let mut end = Self::new(ns, db, sq, g, h).encode_key()?;
		beg.extend_from_slice(&[0x00; 9]);
		end.extend_from_slice(&[0xFF; 9]);
		Ok(beg..end)
	}

	pub(crate) fn new_ba_range(
		ns: NamespaceId,
		db: DatabaseId,
		sq: &'a str,
	) -> Result<Range<Vec<u8>>> {
		Self::new_range(ns, db, b'b', b'a', sq)
	}

	pub(crate) fn new_st_range(
		ns: NamespaceId,
		db: DatabaseId,
		sq: &'a str,
	) -> Result<Range<Vec<u8>>> {
		Self::new_range(ns, db, b's', b't', sq)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn ba_range() {
		let range = Prefix::new_ba_range(NamespaceId(1), DatabaseId(2), "testsq").unwrap();
		assert_eq!(
			range.start,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!sqtestsq\0!ba\0\0\0\0\0\0\0\0\0"
		);
		assert_eq!(
			range.end,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!sqtestsq\0!ba\xff\xff\xff\xff\xff\xff\xff\xff\xff"
		);
	}
}
