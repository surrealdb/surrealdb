//! Sequence Batch Key (`Ib`) for Full-Text Index Document IDs
//!
//! The `Ib` key stores sequence batches for full-text index document IDs. It's
//! part of the distributed sequence mechanism that enables concurrent document
//! ID generation across multiple nodes.
//!
//! ## Key Structure
//! ```no_compile
//! /*{namespace}*{database}*{table}+{index}!ib{start}
//! ```
//!
//! ## Purpose
//! - **Batch Management**: Stores ranges of document IDs that can be allocated by different nodes
//! - **Concurrency**: Enables multiple nodes to generate unique document IDs without conflicts
//! - **Performance**: Reduces contention by pre-allocating ID ranges in batches
//!
//! ## Usage in Full-Text Search
//! The `Ib` key works together with `Id` keys to manage document
//! identification:
//! 1. Document IDs are allocated in batches using distributed sequences
//! 2. Multiple nodes can allocate from different batches simultaneously
//! 3. This enables lock-free ID generation and reduces database contention
//!
//! ## Category
//! - **Category**: `SequenceBatch`
//! - **Domain**: Full-text search document ID management
//!
//! ## Concurrency Benefits
//! - **Lock-free ID Generation**: Nodes can allocate IDs from pre-allocated batches
//! - **Reduced Contention**: Batch-based allocation minimizes database contention
//! - **Scalability**: Multiple nodes can index documents concurrently
//! - **Consistency**: Ensures unique document IDs across the entire cluster
use std::borrow::Cow;
use std::ops::Range;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::sequences::BatchValue;
use crate::kvs::{KVKey, impl_kv_key_storekey};
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Ib<'a> {
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
	pub start: i64,
}

impl_kv_key_storekey!(Ib<'_> => BatchValue);

impl Categorise for Ib<'_> {
	fn categorise(&self) -> Category {
		Category::SequenceBatch
	}
}

impl<'a> Ib<'a> {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		start: i64,
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
			_g: b'b',
			start,
		}
	}

	pub(crate) fn new_range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
	) -> anyhow::Result<Range<Vec<u8>>> {
		let beg = Self::new(ns, db, tb, ix, i64::MIN).encode_key()?;
		let end = Self::new(ns, db, tb, ix, i64::MAX).encode_key()?;
		Ok(beg..end)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn ib_range() {
		let ib_range = Ib::new_range(NamespaceId(1), DatabaseId(2), "testtb", IndexId(3)).unwrap();
		assert_eq!(
			ib_range.start,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!ib\0\0\0\0\0\0\0\0"
		);
		assert_eq!(
			ib_range.end,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!ib\xff\xff\xff\xff\xff\xff\xff\xff"
		);
	}
}
