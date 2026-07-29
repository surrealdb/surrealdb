//! Stores table doc-ID generator state per node.
//!
//! Per-node state key for the table-scoped document-ID sequence
//! (`/*{ns}*{db}*{tb}!ds{nid}`). Mirrors the index-ID generator state
//! ([`crate::key::table::is`]) but drives the shared doc-ID space owned by
//! [`crate::idx::docids`].

use std::borrow::Cow;

use surrealdb_strand::TableName;
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};
use crate::kvs::sequences::SequenceState;

key! {
	/// Key structure for storing table doc-ID generator state per node.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct DocIdGeneratorStateKey<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'd',
		b's',
		pub nid: Uuid,
	}
}

impl_kv_key_storekey!(DocIdGeneratorStateKey<'a> => SequenceState);

impl Categorise for DocIdGeneratorStateKey<'_> {
	fn categorise(&self) -> Category {
		Category::TableDocIdGeneratorState
	}
}

impl<'a> DocIdGeneratorStateKey<'a> {
	/// Creates a new table doc-ID generator state key.
	pub(crate) fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, nid: Uuid) -> Self {
		Self {
			prefix: DatabaseRoot {
				ns,
				db,
			},
			tb: Cow::Borrowed(tb),
			nid,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = DocIdGeneratorStateKey::new(
			NamespaceId(123),
			DatabaseId(234),
			&tb,
			Uuid::from_u128(15),
		);
		let enc = val.encode_key().unwrap();
		assert_eq!(&*enc, b"/*\0\0\0\x7B*\0\0\0\xEA*testtb\0!ds\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x0F");
	}
}
