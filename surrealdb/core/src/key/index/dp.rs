//! DiskANN pending-operation state key.

use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::trees::diskann::DiskAnnPendingState;
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

/// Stores one shard of the distributed-safe pending-operation summary for one DiskANN index.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Dp<'a> {
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
	pub shard: u16,
}

impl_kv_key_storekey!(Dp<'_> => DiskAnnPendingState);

impl<'a> Dp<'a> {
	/// Creates one `!dp` pending-state guard shard key for one DiskANN index.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		shard: u16,
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
			_g: b'p',
			shard,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::index::dr::DiskAnnRecordPendingPrefix;
	use crate::kvs::KVKey;

	#[test]
	fn pending_state_key_is_outside_dr_range() {
		let tb = TableName::from("testtb");
		let key = Dp::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), 7).encode_key().unwrap();
		let range =
			DiskAnnRecordPendingPrefix::range(NamespaceId(1), DatabaseId(2), &tb, IndexId(3))
				.unwrap();
		assert!(!range.start.le(&key) || !key.lt(&range.end));
	}
}
