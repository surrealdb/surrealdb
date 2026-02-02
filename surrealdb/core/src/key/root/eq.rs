use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::doc::AsyncEventRecord;
use crate::key::category::{Categorise, Category};
use crate::kvs::{HlcTimestamp, impl_kv_key_storekey};
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct EventQueue<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub ns: NamespaceId,
	pub db: DatabaseId,
	pub tb: Cow<'a, TableName>,
	pub ev: Cow<'a, str>,
	/// Timestamp when this event was generated (component 1 of the composite unique ID).
	pub ts: u64,
	/// The ID of the node that generated the event (component 2 of the composite unique ID).
	pub node_id: Uuid,
}

impl_kv_key_storekey!(EventQueue<'_> => AsyncEventRecord);

impl Categorise for EventQueue<'_> {
	fn categorise(&self) -> Category {
		Category::EventQueue
	}
}

impl<'a> EventQueue<'a> {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ev: &'a str,
		ts: HlcTimestamp,
		node_id: Uuid,
	) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'e',
			_c: b'q',
			ns,
			db,
			tb: Cow::Borrowed(tb),
			ev: Cow::Borrowed(ev),
			ts: ts.0,
			node_id,
		}
	}

	pub(crate) fn decode_key(k: &[u8]) -> Result<EventQueue<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}

	pub(crate) fn range() -> (Vec<u8>, Vec<u8>) {
		(b"/!eq\0".to_vec(), b"/!eq\0xff".to_vec())
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
		let val = EventQueue::new(NamespaceId(1), DatabaseId(2), &tb, ev, HlcTimestamp(1), id);
		let enc = EventQueue::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/!eq\x00\x00\x00\x01\x00\x00\x00\x02testtb\0testev\0\0\0\0\0\0\0\0\x01\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10"
		);
	}

	#[test]
	fn range() {
		assert_eq!(EventQueue::range(), (b"/!eq\0".to_vec(), b"/!eq\0xff".to_vec()));
	}
}
