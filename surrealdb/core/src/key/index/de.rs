//! Stores vectors and status for a DiskANN index.

use std::borrow::Cow;
use std::ops::Range;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::trees::diskann::{DiskAnnElement, ElementId};
use crate::kvs::{KVKey, Key, impl_kv_key_storekey};
use crate::val::TableName;

/// Stores one DiskANN graph element vector and deleted marker.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct De<'a> {
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
	pub element_id: ElementId,
}

impl_kv_key_storekey!(De<'_> => DiskAnnElement);

impl<'a> De<'a> {
	/// Creates the `!de{element_id}` key for one graph element payload.
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		element_id: ElementId,
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
			_g: b'e',
			element_id,
		}
	}

	/// Returns the range covering all graph element payloads for one DiskANN index.
	pub(crate) fn range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
	) -> Result<Range<Key>> {
		let beg = Self::new(ns, db, tb, ix, 0).encode_key()?;
		let end = Self::new(ns, db, tb, ix, u64::MAX).encode_key()?;
		Ok(beg..end)
	}
}
