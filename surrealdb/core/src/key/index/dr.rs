//! Record-keyed pending updates for DiskANN indexes.

use std::borrow::Cow;
use std::ops::Range;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::trees::diskann::DiskAnnRecordPendingUpdate;
use crate::kvs::{KVKey, Key, impl_kv_key_storekey};
use crate::val::{IndexFormat, RecordIdKey, TableName};

/// Stores the coalesced pending update for one DiskANN indexed record.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "IndexFormat")]
pub(crate) struct DiskAnnRecordPending<'a> {
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

impl KVKey for DiskAnnRecordPending<'_> {
	type ValueType = DiskAnnRecordPendingUpdate;

	fn encode_key(&self) -> Result<Key> {
		Ok(storekey::encode_vec_format::<IndexFormat, _>(self)
			.map_err(|_| crate::err::Error::Unencodable)?)
	}

	fn value_context(&self) {}
}

impl<'a> DiskAnnRecordPending<'a> {
	/// Creates the `!dr{record_key}` pending-operation key.
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
			_f: b'd',
			_g: b'r',
			id: Cow::Borrowed(id),
		}
	}

	/// Decodes a `!dr` key scanned during lookup or compaction.
	pub(crate) fn decode_key(k: &[u8]) -> Result<DiskAnnRecordPending<'_>> {
		Ok(storekey::decode_borrow_format::<IndexFormat, _>(k)?)
	}
}

/// Prefix used to build the range covering all `!dr` pending updates for one DiskANN index.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode)]
#[storekey(format = "()")]
pub(crate) struct DiskAnnRecordPendingPrefix<'a> {
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

impl_kv_key_storekey!(DiskAnnRecordPendingPrefix<'_> => ());

impl<'a> DiskAnnRecordPendingPrefix<'a> {
	/// Returns the range covering record-keyed DiskANN pending updates.
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
			_f: b'd',
			_g: b'r',
		}
		.encode_key()?;
		let mut end = beg.clone();
		beg.push(0);
		end.push(0xff);
		Ok(beg..end)
	}
}
