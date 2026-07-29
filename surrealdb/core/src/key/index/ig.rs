//! Key encoding for appended records during concurrent index builds.
use std::borrow::Cow;
use std::fmt::Debug;

use surrealdb_strand::TableName;

use crate::catalog::IndexId;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::kvs::index::{Appending, AppendingId, BatchId};

key! {
	#[derive(Debug, Clone, PartialEq)]
	pub(crate) struct IndexAppending<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'i',
		b'g',
		pub appending_id: AppendingId,
		pub batch_id: BatchId,
	}
}
impl_kv_key_storekey!(IndexAppending<'a> => Appending);

key! {
	#[derive(Debug, Clone, PartialEq)]
	pub(crate) struct IndexAppendingPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'i',
		b'g',
	}
}
impl_kv_range_storekey!(IndexAppendingPrefix<'_>);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = IndexAppending {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			appending_id: 1,
			batch_id: 2,
		};
		let enc = IndexAppending::encode_key(&val).unwrap();
		assert_eq!(
			&*enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!ig\x00\x00\x00\x01\x00\x00\x00\x02",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
