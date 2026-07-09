//! Stores table doc-ID generator batch allocations.
//!
//! Batch-allocation key for the table-scoped document-ID sequence
//! (`/*{ns}*{db}*{tb}!dh{start}`). Mirrors the index-ID generator
//! ([`crate::key::table::ih`]) but drives the shared doc-ID space owned by
//! [`crate::idx::docids`].

use std::borrow::Cow;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::kvs::sequences::BatchValue;
use crate::val::TableName;

key! {
	/// Key structure for storing table doc-ID generator batch allocations.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct DocIdGeneratorBatchKey<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'd',
		b'h',
		pub start: i64,
	}
}

impl_kv_key_storekey!(DocIdGeneratorBatchKey<'a> => BatchValue);

impl Categorise for DocIdGeneratorBatchKey<'_> {
	fn categorise(&self) -> Category {
		Category::TableDocIdGeneratorBatch
	}
}

impl<'a> DocIdGeneratorBatchKey<'a> {
	/// Creates a new table doc-ID generator batch key.
	pub(crate) fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, start: i64) -> Self {
		Self {
			prefix: DatabaseRoot {
				ns,
				db,
			},
			tb: Cow::Borrowed(tb),
			start,
		}
	}
}

key! {
	/// Prefix over every table doc-ID generator batch on a table.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct DocIdGeneratorBatchPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'd',
		b'h',
	}
}

impl_kv_range_storekey!(DocIdGeneratorBatchPrefix<'_>);

impl<'a> DocIdGeneratorBatchPrefix<'a> {
	/// Creates the prefix covering every doc-ID generator batch on `tb`.
	pub(crate) fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName) -> Self {
		Self {
			prefix: DatabaseRoot {
				ns,
				db,
			},
			tb: Cow::Borrowed(tb),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = DocIdGeneratorBatchKey::new(NamespaceId(123), DatabaseId(234), &tb, 15);
		let enc = val.encode_key().unwrap();
		assert_eq!(&*enc, b"/*\0\0\0\x7B*\0\0\0\xEA*testtb\0!dh\x80\0\0\0\0\0\0\x0F");
	}

	#[test]
	fn prefix() {
		let tb = TableName::from("testtb");
		let pre = DocIdGeneratorBatchPrefix::new(NamespaceId(123), DatabaseId(234), &tb)
			.encode_bound()
			.unwrap();
		assert_eq!(&*pre, b"/*\0\0\0\x7B*\0\0\0\xEA*testtb\0!dh");
	}
}
