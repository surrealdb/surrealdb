//! Index key encoding and prefixes for the KV store.
//!
//! This module defines the on-disk key layout for secondary indexes and helpers
//! to construct prefixes and full keys. Field values are serialized via
//! key::value::Array, which normalizes numeric values across Number
//! variants (Int/Float/Decimal) using a lexicographic encoding so that byte
//! order aligns with numeric order. As a consequence, numerically-equal values
//! (e.g., 0, 0.0, 0dec) map to identical key bytes and are treated as equal by
//! UNIQUE indexes and during scans.
//!
//! Helper functions like prefix_beg/prefix_end/prefix_ids_* build range bounds
//! for scanning the KV store. Keys are designed to be concatenation-friendly,
//! using zero-terminated components where appropriate to ensure parsers stop at
//! the correct boundaries when decoding.
pub mod all;
pub mod dc;
pub mod dl;
pub mod hd;
pub mod he;
pub mod hi;
pub mod hl;
pub mod hs;
pub mod hv;
pub mod ia;
pub mod ib;
pub mod id;
pub mod ii;
pub mod ip;
pub mod is;
pub mod iu;
pub mod td;
pub mod tt;

use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::{KVKey, impl_kv_key_storekey};
use crate::val::{Array, IndexFormat, RecordId, RecordIdKey};

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
			_d: b'+',
			ix,
			_e: b'*',
		}
	}
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "IndexFormat")]
struct PrefixIds<'a> {
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
	/// Encoded index field values. Uses Array which normalizes numeric
	/// types (Int/Float/Decimal) into a lexicographically ordered byte form so
	/// equal numeric values compare equal in index keys.
	pub fd: Cow<'a, Array>,
}

impl crate::kvs::KVKey for PrefixIds<'_> {
	type ValueType = Vec<u8>;
	fn encode_key(&self) -> anyhow::Result<Vec<u8>> {
		Ok(storekey::encode_vec_format::<IndexFormat, _>(self)
			.map_err(|_| crate::err::Error::Unencodable)?)
	}
}

impl<'a> PrefixIds<'a> {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: IndexId, fd: &'a Array) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'+',
			ix,
			_e: b'*',
			fd: Cow::Borrowed(fd),
		}
	}
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "IndexFormat")]
pub(crate) struct Index<'a> {
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
	/// Encoded index field values. Uses Array which normalizes numeric
	/// types (Int/Float/Decimal) into a lexicographically ordered byte form so
	/// equal numeric values compare equal in index keys.
	pub fd: Cow<'a, Array>,
	pub id: Option<Cow<'a, RecordIdKey>>,
}

impl crate::kvs::KVKey for Index<'_> {
	type ValueType = RecordId;
	fn encode_key(&self) -> ::anyhow::Result<Vec<u8>> {
		Ok(storekey::encode_vec_format::<IndexFormat, _>(self)
			.map_err(|_| crate::err::Error::Unencodable)?)
	}
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
		ix: IndexId,
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
			tb: Cow::Borrowed(tb),
			_d: b'+',
			ix,
			_e: b'*',
			fd: Cow::Borrowed(fd),
			id: id.map(Cow::Borrowed),
		}
	}

	fn prefix(ns: NamespaceId, db: DatabaseId, tb: &str, ix: IndexId) -> Result<Vec<u8>> {
		Prefix::new(ns, db, tb, ix).encode_key()
	}

	/// Start of the index keyspace: prefix + 0x00. Used as the lower bound
	/// when iterating all entries for a given index.
	pub fn prefix_beg(ns: NamespaceId, db: DatabaseId, tb: &str, ix: IndexId) -> Result<Vec<u8>> {
		let mut beg = Self::prefix(ns, db, tb, ix)?;
		beg.extend_from_slice(&[0x00]); // lower sentinel for entire index keyspace
		Ok(beg)
	}

	/// End of the index keyspace: prefix + 0xFF. Used as the upper bound (exclusive)
	/// when iterating all entries for a given index.
	pub fn prefix_end(ns: NamespaceId, db: DatabaseId, tb: &str, ix: IndexId) -> Result<Vec<u8>> {
		let mut beg = Self::prefix(ns, db, tb, ix)?;
		beg.extend_from_slice(&[0xff]); // upper sentinel for entire index keyspace (exclusive)
		Ok(beg)
	}

	/// Build the base prefix for an index including the encoded field values.
	/// Field values are encoded using Array which zero-terminates
	/// components so that composite keys can be parsed unambiguously.
	fn prefix_ids(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: IndexId,
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
		ix: IndexId,
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
		ix: IndexId,
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
		ix: IndexId,
		fd: &Array,
	) -> Result<Vec<u8>> {
		let mut beg = Self::prefix_ids(ns, db, tb, ix, fd)?;
		*beg.last_mut().expect("prefix buffer is non-empty") = 0x00; // set trailing sentinel to 0x00 -> inclusive lower bound within composite tuple
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
		ix: IndexId,
		fd: &Array,
	) -> Result<Vec<u8>> {
		let mut beg = Self::prefix_ids(ns, db, tb, ix, fd)?;
		*beg.last_mut().expect("prefix buffer is non-empty") = 0xff; // set trailing sentinel to 0xFF -> exclusive upper bound within composite tuple
		Ok(beg)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::Array;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let fd: Array = vec!["testfd1", "testfd2"].into();
		let id = RecordIdKey::String("testid".into());
		let val = Index::new(NamespaceId(1), DatabaseId(2), "testtb", IndexId(3), &fd, Some(&id));
		let enc = Index::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03*\x06testfd1\0\x06testfd2\0\0\x03\x03testid\0"
		);
	}

	#[test]
	fn key_none() {
		let fd: Array = vec!["testfd1", "testfd2"].into();
		let val = Index::new(NamespaceId(1), DatabaseId(2), "testtb", IndexId(3), &fd, None);
		let enc = Index::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\0\0\0\x01*\0\0\0\x02*testtb\0+\0\0\0\x03*\x06testfd1\0\x06testfd2\0\0\x02"
		);
	}

	#[test]
	fn check_composite() {
		let fd: Array = vec!["testfd1"].into();

		let enc = Index::prefix_ids_composite_beg(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			IndexId(3),
			&fd,
		)
		.unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03*\x06testfd1\0\x00"
		);

		let enc = Index::prefix_ids_composite_end(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			IndexId(3),
			&fd,
		)
		.unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03*\x06testfd1\0\xff"
		);
	}
}
