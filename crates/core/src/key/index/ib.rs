//! Stores sequence batches for FullTextIndex / DocIDs
use crate::key::category::{Categorise, Category};
use crate::kvs::{KeyEncode, impl_key};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Ib<'a> {
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
	pub start: i64,
}
impl_key!(Ib<'a>);

impl Categorise for Ib<'_> {
	fn categorise(&self) -> Category {
		Category::SequenceBatch
	}
}

impl<'a> Ib<'a> {
	pub(crate) fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, start: i64) -> Self {
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
			_g: b'b',
			start,
		}
	}

	pub(crate) fn new_range(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
	) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
		let beg = Self::new(ns, db, tb, ix, i64::MIN).encode()?;
		let end = Self::new(ns, db, tb, ix, i64::MAX).encode()?;
		Ok((beg, end))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn ib_range() {
		let (beg, end) = Ib::new_range("testns", "testdb", "testtb", "testix").unwrap();
		assert_eq!(beg, b"/*testns\0*testdb\0*testtb\0+testix\0!ib\0\0\0\0\0\0\0\0");
		assert_eq!(
			end,
			b"/*testns\0*testdb\0*testtb\0+testix\0!ib\xff\xff\xff\xff\xff\xff\xff\xff"
		);
	}
}
