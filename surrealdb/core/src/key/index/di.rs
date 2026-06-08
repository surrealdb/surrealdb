//! Stores RecordId to DocId mappings for a DiskANN index.

use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::val::{IndexFormat, RecordIdKey, TableName};

/// Maps a SurrealDB record key to its compact DiskANN document ID.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "IndexFormat")]
pub(crate) struct Di<'a> {
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

impl crate::kvs::KVKey for Di<'_> {
	type ValueType = u64;

	fn encode_key(&self) -> anyhow::Result<Vec<u8>> {
		Ok(storekey::encode_vec_format::<IndexFormat, _>(self)
			.map_err(|_| crate::err::Error::Unencodable)?)
	}

	fn value_context(&self) {}
}

impl<'a> Di<'a> {
	/// Creates the `!di{record_key}` lookup key for one record.
	pub fn new(
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
			_g: b'i',
			id: Cow::Borrowed(id),
		}
	}
}
