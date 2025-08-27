//! Stores sequence states
pub mod ba;
pub mod st;

use std::ops::Range;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Prefix<'a> {
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
}

impl KVKey for Prefix<'_> {
	type ValueType = Vec<u8>;
}

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
			sq,
			_f: b'!',
			_g: g,
			_h: h,
		}
	}

	pub(crate) fn new_ba_range(
		ns: NamespaceId,
		db: DatabaseId,
		sq: &'a str,
	) -> Result<Range<Vec<u8>>> {
		let mut beg = Self::new(ns, db, sq, b'b', b'a').encode_key()?;
		let mut end = Self::new(ns, db, sq, b'b', b'a').encode_key()?;
		beg.extend_from_slice(&[0x00; 9]);
		end.extend_from_slice(&[0xFF; 9]);
		Ok(beg..end)
	}

	pub(crate) fn new_st_range(
		ns: NamespaceId,
		db: DatabaseId,
		sq: &'a str,
	) -> Result<Range<Vec<u8>>> {
		let mut beg = Self::new(ns, db, sq, b's', b't').encode_key()?;
		let mut end = Self::new(ns, db, sq, b's', b't').encode_key()?;
		beg.extend_from_slice(&[0x00; 9]);
		end.extend_from_slice(&[0xFF; 9]);
		Ok(beg..end)
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
