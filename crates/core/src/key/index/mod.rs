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
pub mod df;
pub mod hd;
pub mod he;
pub mod hi;
pub mod hl;
pub mod hs;
pub mod hv;
pub mod ib;
pub mod ip;
pub mod iu;
pub mod vm;

use crate::err::Error;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::{impl_key, KeyEncode};
use crate::sql::array::Array;
use crate::sql::id::Id;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
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
impl_key!(Prefix<'a>);

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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
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
impl_key!(PrefixIds<'a>);

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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
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
impl_key!(Index<'a>);

impl Categorise for Index<'_> {
	fn categorise(&self) -> Category {
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

	fn prefix(ns: &str, db: &str, tb: &str, ix: &str) -> Result<Vec<u8>, Error> {
		Prefix::new(ns, db, tb, ix).encode()
	}

	pub fn prefix_beg(ns: &str, db: &str, tb: &str, ix: &str) -> Result<Vec<u8>, Error> {
		let mut beg = Self::prefix(ns, db, tb, ix)?;
		beg.extend_from_slice(&[0x00]); // lower sentinel for entire index keyspace
		Ok(beg)
	}

	pub fn prefix_end(ns: &str, db: &str, tb: &str, ix: &str) -> Result<Vec<u8>, Error> {
		let mut beg = Self::prefix(ns, db, tb, ix)?;
		beg.extend_from_slice(&[0xff]); // upper sentinel for entire index keyspace (exclusive)
		Ok(beg)
	}

	fn prefix_ids(ns: &str, db: &str, tb: &str, ix: &str, fd: &Array) -> Result<Vec<u8>, Error> {
		PrefixIds::new(ns, db, tb, ix, fd).encode()
	}

	pub fn prefix_ids_beg(
		ns: &str,
		db: &str,
		tb: &str,
		ix: &str,
		fd: &Array,
	) -> Result<Vec<u8>, Error> {
		let mut beg = Self::prefix_ids(ns, db, tb, ix, fd)?;
		beg.extend_from_slice(&[0x00]);
		Ok(beg)
	}

	pub fn prefix_ids_end(
		ns: &str,
		db: &str,
		tb: &str,
		ix: &str,
		fd: &Array,
	) -> Result<Vec<u8>, Error> {
		let mut beg = Self::prefix_ids(ns, db, tb, ix, fd)?;
		beg.extend_from_slice(&[0xff]);
		Ok(beg)
	}

	pub fn prefix_ids_composite_beg(
		ns: &str,
		db: &str,
		tb: &str,
		ix: &str,
		fd: &Array,
	) -> Result<Vec<u8>, Error> {
		let mut beg = Self::prefix_ids(ns, db, tb, ix, fd)?;
		*beg.last_mut().unwrap() = 0x00; // set trailing sentinel to 0x00 -> inclusive lower bound within composite tuple
		Ok(beg)
	}

	pub fn prefix_ids_composite_end(
		ns: &str,
		db: &str,
		tb: &str,
		ix: &str,
		fd: &Array,
	) -> Result<Vec<u8>, Error> {
		let mut beg = Self::prefix_ids(ns, db, tb, ix, fd)?;
		*beg.last_mut().unwrap() = 0xff; // set trailing sentinel to 0xFF -> exclusive upper bound within composite tuple
		Ok(beg)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::{KeyDecode, KeyEncode};

	#[test]
	fn key() {
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

	#[test]
	fn key_none() {
		let fd = vec!["testfd1", "testfd2"].into();
		let val = Index::new("testns", "testdb", "testtb", "testix", &fd, None);
		let enc = Index::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0+testix\0*\0\0\0\x04testfd1\0\0\0\0\x04testfd2\0\x01\0"
		);
	}

	#[test]
	fn check_composite() {
		let fd = vec!["testfd1"].into();

		let enc =
			Index::prefix_ids_composite_beg("testns", "testdb", "testtb", "testix", &fd).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0*\0\0\0\x04testfd1\0\x00");

		let enc =
			Index::prefix_ids_composite_end("testns", "testdb", "testtb", "testix", &fd).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0*\0\0\0\x04testfd1\0\xff");
	}
}
