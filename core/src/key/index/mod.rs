//! Stores an index entry
pub mod all;
pub mod bc;
pub mod bd;
pub mod bf;
pub mod bi;
pub mod bk;
pub mod bl;
pub mod bo;
pub mod bp;
pub mod bs;
pub mod bt;
pub mod bu;
pub mod vm;

use crate::key::category::Category;
use crate::key::key_req::KeyRequirements;
use crate::sql::array::Array;
use crate::sql::id::Id;
use derive::Key;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::ops::Range;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
struct Prefix<'a> {
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
}

impl<'a> Prefix<'a> {
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
			_e: b'*',
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
struct PrefixIds<'a> {
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
	pub fd: Cow<'a, Array>,
}

impl<'a> PrefixIds<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, fd: &'a Array) -> Self {
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
			_e: b'*',
			fd: Cow::Borrowed(fd),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Index<'a> {
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
	pub fd: Cow<'a, Array>,
	pub id: Option<Cow<'a, Id>>,
}

impl KeyRequirements for Index<'_> {
	fn key_category(&self) -> Category {
		Category::Index
	}
}

impl<'a> Index<'a> {
	pub fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		fd: &'a Array,
		id: Option<&'a Id>,
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
			_e: b'*',
			fd: Cow::Borrowed(fd),
			id: id.map(Cow::Borrowed),
		}
	}

	fn prefix(ns: &str, db: &str, tb: &str, ix: &str) -> Vec<u8> {
		Prefix::new(ns, db, tb, ix).encode().unwrap()
	}

	pub fn prefix_beg(ns: &str, db: &str, tb: &str, ix: &str) -> Vec<u8> {
		let mut beg = Self::prefix(ns, db, tb, ix);
		beg.extend_from_slice(&[0x00]);
		beg
	}

	pub fn prefix_end(ns: &str, db: &str, tb: &str, ix: &str) -> Vec<u8> {
		let mut beg = Self::prefix(ns, db, tb, ix);
		beg.extend_from_slice(&[0xff]);
		beg
	}

	pub fn range(ns: &str, db: &str, tb: &str, ix: &str) -> Range<Vec<u8>> {
		Self::prefix_beg(ns, db, tb, ix)..Self::prefix_end(ns, db, tb, ix)
	}

	fn prefix_ids(ns: &str, db: &str, tb: &str, ix: &str, fd: &Array) -> Vec<u8> {
		PrefixIds::new(ns, db, tb, ix, fd).encode().unwrap()
	}

	pub fn prefix_ids_beg(ns: &str, db: &str, tb: &str, ix: &str, fd: &Array) -> Vec<u8> {
		let mut beg = Self::prefix_ids(ns, db, tb, ix, fd);
		beg.extend_from_slice(&[0x00]);
		beg
	}

	pub fn prefix_ids_end(ns: &str, db: &str, tb: &str, ix: &str, fd: &Array) -> Vec<u8> {
		let mut beg = Self::prefix_ids(ns, db, tb, ix, fd);
		beg.extend_from_slice(&[0xff]);
		beg
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let fd = vec!["testfd1", "testfd2"].into();
		let id = "testid".into();
		let val = Index::new("testns", "testdb", "testtb", "testix", &fd, Some(&id));
		let enc = Index::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0+testix\0*\0\0\0\x04testfd1\0\0\0\0\x04testfd2\0\x01\x01\0\0\0\x01testid\0"
		);

		let dec = Index::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
