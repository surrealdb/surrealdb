//! Stores a LIVE SELECT query definition on the table
use std::borrow::Cow;

use anyhow::Result;
use uuid::Uuid;

use crate::catalog::SubscriptionDefinition;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::val::TableName;

key! {
	/// Lv is used to track a live query and is cluster independent, i.e. it is tied
	/// with a ns/db/tb combo without the cl. The live statement includes the node
	/// id, so lq can be derived purely from an lv.
	///
	/// The value of the lv is the statement.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Lq<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'l',
		b'q',
		pub lq: Uuid,
	}
}

impl_kv_key_storekey!(Lq<'a> => SubscriptionDefinition);

impl Categorise for Lq<'_> {
	fn categorise(&self) -> Category {
		Category::TableLiveQuery
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct LqPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'l',
		b'q',
	}
}

impl_kv_range_storekey!(LqPrefix<'_>);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let live_query_id =
			Uuid::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
		let tb = TableName::from("testtb");
		let val = Lq {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			lq: live_query_id,
		};
		let enc = Lq::encode_key(&val).unwrap();
		assert_eq!(
			&*enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!lq\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10"
		);
	}

	#[test]
	fn prefix() {
		let tb = TableName::from("testtb");
		let val = LqPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
		}
		.encode_range()
		.unwrap();
		assert_eq!(val.start.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!lq\x00");
		assert_eq!(val.end.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!lr");
	}
}
