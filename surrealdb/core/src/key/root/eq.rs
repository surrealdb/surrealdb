use std::borrow::Cow;

use anyhow::Result;
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::doc::AsyncEventRecord;
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::val::TableName;

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct EventQueue<'a> {
		b'/',
		b'!',
		b'e',
		b'q',
		pub ns: NamespaceId,
		pub db: DatabaseId,
		pub tb: Cow<'a, TableName>,
		pub ev: Cow<'a, str>,
		/// Timestamp when this event was generated (component 1 of the composite unique ID).
		pub ts: u64,
		/// The ID of the node that generated the event (component 2 of the composite unique ID).
		pub node_id: Uuid,
	}
}

impl_kv_key_storekey!(EventQueue<'a> => AsyncEventRecord);

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct EventQueuePrefix {
		b'/',
		b'!',
		b'e',
		b'q',
	}
}
impl_kv_range_storekey!(EventQueuePrefix);

impl Categorise for EventQueue<'_> {
	fn categorise(&self) -> Category {
		Category::EventQueue
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVKey;

	#[test]
	fn key() {
		let id = Uuid::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
		let tb = TableName::from("testtb");
		let val = EventQueue {
			ns: NamespaceId(1),
			db: DatabaseId(2),
			tb: Cow::Borrowed(&tb),
			ev: Cow::Borrowed("testev"),
			ts: 1,
			node_id: id,
		};
		let enc = EventQueue::encode_key(&val).unwrap();
		assert_eq!(
			enc.as_slice(),
			b"/!eq\x00\x00\x00\x01\x00\x00\x00\x02testtb\0testev\0\0\0\0\0\0\0\0\x01\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10"
		);
	}
}
