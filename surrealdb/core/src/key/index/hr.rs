//! Record-keyed pending updates for HNSW indexes.

use std::borrow::Cow;
use std::ops::Range;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::trees::hnsw::HnswRecordPendingUpdate;
use crate::kvs::{KVKey, Key, impl_kv_key_storekey};
use crate::val::{IndexFormat, RecordIdKey, TableName};

/// Pending HNSW update keyed by the owning record.
///
/// There is at most one live `!hr` key for a record. Repeated writes replace
/// the desired vectors while preserving the original graph baseline, making
/// pending HNSW work independent of cross-node append ordering.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "IndexFormat")]
pub(crate) struct HnswRecordPending<'a> {
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
	pub id: Cow<'a, RecordIdKey>,
}

impl KVKey for HnswRecordPending<'_> {
	type ValueType = HnswRecordPendingUpdate;

	fn encode_key(&self) -> Result<Key> {
		Ok(storekey::encode_vec_format::<IndexFormat, _>(self)
			.map_err(|_| crate::err::Error::Unencodable)?)
	}

	fn value_context(&self) {}
}

impl<'a> HnswRecordPending<'a> {
	/// Creates the pending key for one record.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		id: &'a RecordIdKey,
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
			_g: b'r',
			id: Cow::Borrowed(id),
		}
	}

	/// Decodes a stored pending key into its record identity.
	pub(crate) fn decode_key(k: &[u8]) -> Result<HnswRecordPending<'_>> {
		Ok(storekey::decode_borrow_format::<IndexFormat, _>(k)?)
	}
}

/// Prefix for all record-keyed HNSW pending updates for an index.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode)]
#[storekey(format = "()")]
pub(crate) struct HnswRecordPendingPrefix<'a> {
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

impl_kv_key_storekey!(HnswRecordPendingPrefix<'_> => ());

impl<'a> HnswRecordPendingPrefix<'a> {
	/// Returns the key range containing all `!hr` pending updates for an index.
	pub(crate) fn range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
	) -> Result<Range<Key>> {
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
			_g: b'r',
		}
		.encode_key()?;
		let mut end = beg.clone();
		beg.push(0);
		end.push(0xff);
		Ok(beg..end)
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::key::index::hp::HnswPendingPrefix;

	#[test]
	fn record_pending_key_is_outside_hp_range() {
		let tb = TableName::from("testtb");
		let id = RecordIdKey::String(Strand::new_static("testid"));
		let key = HnswRecordPending::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), &id)
			.encode_key()
			.unwrap();
		let range =
			HnswPendingPrefix::range(NamespaceId(1), DatabaseId(2), &tb, IndexId(3)).unwrap();
		assert!(!range.start.le(&key) || !key.lt(&range.end));
	}
}
