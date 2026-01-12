//! Index Count State Key Structure
//!
//! This module defines the `IndexCountKey` key used to track incremental
//! changes to the total number of indexed records for a given secondary
//! index. Instead of updating a single counter in place (which would cause
//! contention), SurrealDB appends small delta entries that are periodically
//! compacted into a single aggregate entry.
//!
//! Purpose
//! - Record point-in-time deltas (+N / -N) to the count of items referenced by an index on a
//!   specific table.
//! - Allow fast COUNT operations by summing deltas, and enable background compaction to collapse
//!   many deltas into one.
//!
//! Key pattern
//! - Prefix: `/*{ns}*{db}*{tb}+{ix}!iu`
//! - Suffix: `[{uid}] {pos} {count}`
//!   - `uid`: Optional pair of UUIDs uniquely identifying the origin of the delta. For normal
//!     update operations the pair is `(actor_id, event_id)`; for compacted keys it is `None`.
//!   - `pos`: Whether the delta is positive (`true`) or negative (`false`).
//!   - `count`: Magnitude of the delta (unsigned 64-bit integer).
//!
//! When compacted, all per-event delta entries within the index range are
//! removed and a single entry with `uid = None` is written carrying the net
//! count with the appropriate `pos` value.

use std::borrow::Cow;
use std::ops::Range;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::{KVKey, impl_kv_key_storekey};
use crate::val::TableName;

/// A key representing a delta applied to the total item count of an index.
///
/// Fields
/// - `ns`, `db`, `tb`, `ix`: Identify the namespace, database, table and index this count entry
///   belongs to.
/// - `uid`: Optional pair `(actor_id, event_id)` used to uniquely identify a delta written during a
///   specific operation. `None` is reserved for compacted/aggregated entries.
/// - `pos`: Direction of the delta: `true` for a positive increment, `false` for a decrement.
/// - `count`: Magnitude of the delta.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct IndexCountKey<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
	pub uid: Option<(Uuid, Uuid)>,
	pub pos: bool,
	pub count: u64,
}

impl_kv_key_storekey!(IndexCountKey<'_> => ());

impl Categorise for IndexCountKey<'_> {
	fn categorise(&self) -> Category {
		Category::IndexCountState
	}
}

impl<'a> IndexCountKey<'a> {
	/// Create a new index count delta entry.
	///
	/// Parameters
	/// - `ns`, `db`, `tb`, `ix`: Identify the target index.
	/// - `uid`: Optional origin identifiers; provide `Some((actor_id, event_id))` for normal
	///   per-operation entries, or `None` when writing the compacted aggregate entry.
	/// - `pos`: `true` for a positive delta, `false` for a negative one.
	/// - `count`: Magnitude of the delta.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		uid: Option<(Uuid, Uuid)>,
		pos: bool,
		count: u64,
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
			_e: b'!',
			_f: b'i',
			_g: b'u',
			uid,
			pos,
			count,
		}
	}

	/// Decode a borrowed key slice into an `IndexCountKey`.
	pub(crate) fn decode_key(k: &[u8]) -> Result<IndexCountKey<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}

	/// Compute the inclusive range covering all count entries for a given index.
	///
	/// The returned range spans all possible delta entries (including compacted
	/// ones) under the prefix `/*{ns}*{db}*{tb}+{ix}!iu`.
	pub(crate) fn range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
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
#[storekey(format = "()")]
struct Prefix<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
}

impl_kv_key_storekey!(Prefix<'_> => ());

impl<'a> Prefix<'a> {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, ix: IndexId) -> Self {
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
		let tb = TableName::from("testtb");
		let val = IndexCountKey::new(
			NamespaceId(1),
			DatabaseId(2),
			&tb,
			IndexId(3),
			Some((
				Uuid::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
				Uuid::from_bytes([15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
			)),
			true,
			65535,
		);
		let enc = IndexCountKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!iu\x03\0\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x0f\x0e\x0d\x0c\x0b\x0a\x09\x08\x07\x06\x05\x04\x03\x02\x01\0\x03\0\0\0\0\0\0\xff\xff", "key");
	}

	#[test]
	fn compacted_key() {
		let tb = TableName::from("testtb");
		let val =
			IndexCountKey::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), None, true, 65535);
		let enc = IndexCountKey::encode_key(&val).unwrap();
		assert_eq!(
			enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!iu\x02\x03\0\0\0\0\0\0\xff\xff",
			"compacted key"
		);
	}

	#[test]
	fn range() {
		let tb = TableName::from("testtb");
		let r = IndexCountKey::range(NamespaceId(1), DatabaseId(2), &tb, IndexId(3)).unwrap();
		assert_eq!(
			r.start, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!iu\0",
			"start"
		);
		assert_eq!(r.end, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!iu\xff", "end");
	}
}
