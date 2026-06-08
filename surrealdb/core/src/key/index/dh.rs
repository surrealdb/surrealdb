//! Stores hashed vector to document mappings for a DiskANN index.

use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::trees::diskann::docs::DiskAnnElementHashedDocs;
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

/// Maps a vector hash to one or more full-vector DiskANN document mappings.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Dh<'a> {
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
	pub hash: [u8; 32],
}

impl_kv_key_storekey!(Dh<'_> => DiskAnnElementHashedDocs);

impl<'a> Dh<'a> {
	/// Creates the `!dh{hash}` key for one hashed vector bucket.
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		hash: [u8; 32],
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
			_g: b'h',
			hash,
		}
	}
}
