//! Stores sequence states
pub mod ba;
pub mod st;

use crate::kvs::{impl_key, KeyEncode};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Prefix<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub sq: &'a str,
	_f: u8,
	_g: u8,
	_h: u8,
}
impl_key!(Prefix<'a>);

impl<'a> Prefix<'a> {
	fn new(ns: &'a str, db: &'a str, sq: &'a str, g: u8, h: u8) -> Self {
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
		ns: &'a str,
		db: &'a str,
		sq: &'a str,
	) -> Result<(Vec<u8>, Vec<u8>)> {
		let mut beg = Self::new(ns, db, sq, b'b', b'a').encode()?;
		let mut end = Self::new(ns, db, sq, b'b', b'a').encode()?;
		beg.extend_from_slice(&[0x00; 9]);
		end.extend_from_slice(&[0xFF; 9]);
		Ok((beg, end))
	}

	pub(crate) fn new_st_range(
		ns: &'a str,
		db: &'a str,
		sq: &'a str,
	) -> Result<(Vec<u8>, Vec<u8>)> {
		let mut beg = Self::new(ns, db, sq, b's', b't').encode()?;
		let mut end = Self::new(ns, db, sq, b's', b't').encode()?;
		beg.extend_from_slice(&[0x00; 9]);
		end.extend_from_slice(&[0xFF; 9]);
		Ok((beg, end))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn ba_range() {
		let (beg, end) = Prefix::new_ba_range("testns", "testdb", "testsq").unwrap();
		assert_eq!(beg, b"/*testns\0*testdb\0!sqtestsq\0!ba\0\0\0\0\0\0\0\0\0");
		assert_eq!(end, b"/*testns\0*testdb\0!sqtestsq\0!ba\xff\xff\xff\xff\xff\xff\xff\xff\xff");
	}
}
