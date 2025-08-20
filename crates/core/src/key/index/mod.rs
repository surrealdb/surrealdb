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
pub mod dc;
pub mod dl;
pub mod hd;
pub mod he;
pub mod hi;
pub mod hl;
pub mod hs;
pub mod hv;
#[cfg(not(target_family = "wasm"))]
pub mod ia;
pub mod ib;
pub mod id;
pub mod ii;
#[cfg(not(target_family = "wasm"))]
pub mod ip;
pub mod is;
pub mod td;
pub mod tt;
pub mod vm;

use std::borrow::Cow;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use crate::val::{Array, RecordId, RecordIdKey};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
struct Prefix<'a> {
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
}

impl KVKey for Prefix<'_> {
	type ValueType = Vec<u8>;
}

impl<'a> Prefix<'a> {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: &'a str) -> Self {
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
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	pub fd: Cow<'a, Array>,
}

impl KVKey for PrefixIds<'_> {
	type ValueType = Vec<u8>;
}

impl<'a> PrefixIds<'a> {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: &'a str, fd: &'a Array) -> Self {
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
pub(crate) struct Index<'a> {
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
	pub fd: Cow<'a, Array>,
	pub id: Option<Cow<'a, RecordIdKey>>,
}

impl KVKey for Index<'_> {
	type ValueType = RecordId;
}

impl Categorise for Index<'_> {
	fn categorise(&self) -> Category {
		Category::Index
	}
}

impl<'a> Index<'a> {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		ix: &'a str,
		fd: &'a Array,
		id: Option<&'a RecordIdKey>,
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

	fn prefix(ns: NamespaceId, db: DatabaseId, tb: &str, ix: &str) -> Result<Vec<u8>> {
		Prefix::new(ns, db, tb, ix).encode_key()
	}

	pub fn prefix_beg(ns: NamespaceId, db: DatabaseId, tb: &str, ix: &str) -> Result<Vec<u8>> {
		let mut beg = Self::prefix(ns, db, tb, ix)?;
		beg.extend_from_slice(&[0x00]);
		Ok(beg)
	}

	pub fn prefix_end(ns: NamespaceId, db: DatabaseId, tb: &str, ix: &str) -> Result<Vec<u8>> {
		let mut beg = Self::prefix(ns, db, tb, ix)?;
		beg.extend_from_slice(&[0xff]);
		Ok(beg)
	}

	fn prefix_ids(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: &str,
		fd: &Array,
	) -> Result<Vec<u8>> {
		PrefixIds::new(ns, db, tb, ix, fd).encode_key()
	}

	/// Returns the smallest possible key for the given index field prefix (fd),
	/// used as the inclusive lower bound of a scan over all record ids matching
	/// that prefix. This is equivalent to prefix_ids(...) followed by a 0x00
	/// byte, so that range scans using [beg, end) style boundaries include the
	/// first key.
	pub fn prefix_ids_beg(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: &str,
		fd: &Array,
	) -> Result<Vec<u8>> {
		let mut beg = Self::prefix_ids(ns, db, tb, ix, fd)?;
		beg.extend_from_slice(&[0x00]);
		Ok(beg)
	}

	/// Returns the greatest possible key for the given index field prefix (fd),
	/// typically used as the exclusive upper bound of a scan over all record
	/// ids matching that prefix. This is equivalent to prefix_ids(...)
	/// followed by a 0xff byte so that range scans using [beg, end) do not
	/// include keys beyond the intended prefix.
	pub fn prefix_ids_end(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: &str,
		fd: &Array,
	) -> Result<Vec<u8>> {
		let mut beg = Self::prefix_ids(ns, db, tb, ix, fd)?;
		beg.extend_from_slice(&[0xff]);
		Ok(beg)
	}

	/// Returns the smallest key within the composite index tuple identified by
	/// `fd`. For composite indexes, the last byte acts as a sentinel; setting
	/// it to 0x00 gives the inclusive lower bound when scanning for an exact
	/// composite match.
	pub fn prefix_ids_composite_beg(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: &str,
		fd: &Array,
	) -> Result<Vec<u8>> {
		let mut beg = Self::prefix_ids(ns, db, tb, ix, fd)?;
		*beg.last_mut().unwrap() = 0x00;
		Ok(beg)
	}

	/// Returns the greatest key within the composite index tuple identified by
	/// `fd`. For composite indexes, the last byte acts as a sentinel; setting
	/// it to 0xff yields the exclusive upper bound for scans targeting the
	/// exact composite value.
	pub fn prefix_ids_composite_end(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: &str,
		fd: &Array,
	) -> Result<Vec<u8>> {
		let mut beg = Self::prefix_ids(ns, db, tb, ix, fd)?;
		*beg.last_mut().unwrap() = 0xff;
		Ok(beg)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let fd = vec!["testfd1", "testfd2"].into();
		let id = RecordIdKey::String("testid".to_owned());
		let val = Index::new(NamespaceId(1), DatabaseId(2), "testtb", "testix", &fd, Some(&id));
		let enc = Index::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0*\0\0\0\x04testfd1\0\0\0\0\x04testfd2\0\x01\x01\0\0\0\x01testid\0"
		);
	}

	#[test]
	fn key_none() {
		let fd = vec!["testfd1", "testfd2"].into();
		let val = Index::new(NamespaceId(1), DatabaseId(2), "testtb", "testix", &fd, None);
		let enc = Index::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0*\0\0\0\x04testfd1\0\0\0\0\x04testfd2\0\x01\0"
		);
	}

	#[test]
	fn check_composite() {
		let fd = vec!["testfd1"].into();

		let enc =
			Index::prefix_ids_composite_beg(NamespaceId(1), DatabaseId(2), "testtb", "testix", &fd)
				.unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0*\0\0\0\x04testfd1\0\x00"
		);

		let enc =
			Index::prefix_ids_composite_end(NamespaceId(1), DatabaseId(2), "testtb", "testix", &fd)
				.unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0*\0\0\0\x04testfd1\0\xff"
		);
	}
}
