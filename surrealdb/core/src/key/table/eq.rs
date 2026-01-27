use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::doc::AsyncEventRecord;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Eq<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	_e: u8,
	_f: u8,
	pub ev: Cow<'a, str>,
	/// Timestamp when this event was generated (Component 1 of the unique ID)
	pub ts_id: u64,
	/// Unique event id within the node that generated the event (Component 2 of the composite
	/// unique ID)
	pub event_id: u64,
	/// The id of the node that generated the event (Component 3 of the composite unique ID)
	pub node_id: Uuid,
}

impl_kv_key_storekey!(Eq<'_> => AsyncEventRecord);

impl Categorise for Eq<'_> {
	fn categorise(&self) -> Category {
		Category::TableEventQueue
	}
}

impl<'a> Eq<'a> {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ev: &'a str,
		ts_id: u64,
		event_id: u64,
		node_id: Uuid,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'!',
			_e: b'e',
			_f: b'q',
			ev: Cow::Borrowed(ev),
			ts_id,
			event_id,
			node_id,
		}
	}

	pub fn _decode_key(k: &[u8]) -> Result<Eq<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let id = Uuid::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
		let tb = TableName::from("testtb");
		let ev = "testev";
		let val = Eq::new(NamespaceId(1), DatabaseId(2), &tb, ev, 1, 2, id);
		let enc = Eq::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!eqtestev\0\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x02\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10"
		);
	}
}
