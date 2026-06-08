//! Stores DiskANN neighbor lists.

use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::trees::diskann::{DiskAnnNode, ElementId};
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

/// Stores one DiskANN graph adjacency list.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Dn<'a> {
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

impl_kv_key_storekey!(Dn<'_> => DiskAnnNode);

impl<'a> Dn<'a> {
	/// Creates the `!dn{element_id}` key for one graph node's neighbors.
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
			_g: b'n',
			element_id,
		}
	}
}
