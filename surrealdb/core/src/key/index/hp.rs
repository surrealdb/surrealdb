//! Append-keyed pending update range for HNSW indexes.
//!
//! HNSW compaction and query-time pending searches read this range when
//! applying stored append-keyed pending values.

use std::borrow::Cow;
use std::ops::Range;

use anyhow::Result;
use storekey::Encode;

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::trees::hnsw::VectorPendingUpdate;
use crate::kvs::{KVKey, impl_kv_key_storekey};
use crate::val::TableName;

/// One append-keyed HNSW pending update, addressed by its append sequence.
///
/// Nothing writes this layout: an index still carrying entries under it was
/// upgraded from before the record-keyed `!hr` layout, and the read paths keep
/// scoring and draining them until the range is empty. Individual keys are
/// therefore only ever constructed by the tests that stage such an index;
/// production code reaches the range through [`HnswPendingPrefix::range`].
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode)]
#[storekey(format = "()")]
#[allow(dead_code)]
pub(crate) struct HnswPendingLegacy<'a> {
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
	pub appending_id: u64,
}

impl_kv_key_storekey!(HnswPendingLegacy<'_> => VectorPendingUpdate);

impl<'a> HnswPendingLegacy<'a> {
	/// Creates the append-keyed pending key at one append sequence.
	#[allow(dead_code)]
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		appending_id: u64,
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
			_f: b'h',
			_g: b'p',
			appending_id,
		}
	}
}

/// Prefix for append-keyed HNSW pending updates for an index.
///
/// Values in this range are encoded as `VectorPendingUpdate` and are consumed
/// by scans over the range rather than by addressing individual keys.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode)]
#[storekey(format = "()")]
pub(crate) struct HnswPendingPrefix<'a> {
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

impl_kv_key_storekey!(HnswPendingPrefix<'_> => ());

impl<'a> HnswPendingPrefix<'a> {
	/// Returns the key range containing append-keyed pending updates.
	pub(crate) fn range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
	) -> Result<Range<Vec<u8>>> {
		let mut beg = Self {
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
			_f: b'h',
			_g: b'p',
		}
		.encode_key()?;
		let mut end = beg.clone();
		beg.push(0);
		end.push(0xff);
		Ok(beg..end)
	}
}
