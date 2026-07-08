//! Sequence Batch Key (`Ib`) for Full-Text Index Document IDs
//!
//! The `Ib` key stores sequence batches for full-text index document IDs. It's
//! part of the distributed sequence mechanism that enables concurrent document
//! ID generation across multiple nodes.
//!
//! ## Key Structure
//! ```no_compile
//! /*{namespace}*{database}*{table}+{index}!ib{start}
//! ```
//!
//! ## Purpose
//! - **Batch Management**: Stores ranges of document IDs that can be allocated by different nodes
//! - **Concurrency**: Enables multiple nodes to generate unique document IDs without conflicts
//! - **Performance**: Reduces contention by pre-allocating ID ranges in batches
//!
//! ## Usage in Full-Text Search
//! The `Ib` key works together with `Id` keys to manage document
//! identification:
//! 1. Document IDs are allocated in batches using distributed sequences
//! 2. Multiple nodes can allocate from different batches simultaneously
//! 3. This enables lock-free ID generation and reduces database contention
//!
//! ## Category
//! - **Category**: `SequenceBatch`
//! - **Domain**: Full-text search document ID management
//!
//! ## Concurrency Benefits
//! - **Lock-free ID Generation**: Nodes can allocate IDs from pre-allocated batches
//! - **Reduced Contention**: Batch-based allocation minimizes database contention
//! - **Scalability**: Multiple nodes can index documents concurrently
//! - **Consistency**: Ensures unique document IDs across the entire cluster
use std::borrow::Cow;

use crate::catalog::IndexId;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::kvs::sequences::BatchValue;
use crate::val::TableName;

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
		pub(crate) struct Ib<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'i',
		b'b',
		pub start: i64,
	}
}

impl_kv_key_storekey!(Ib<'a> => BatchValue);

impl Categorise for Ib<'_> {
	fn categorise(&self) -> Category {
		Category::SequenceBatch
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
		pub(crate) struct IbPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'i',
		b'b',
	}
}

impl_kv_range_storekey!(IbPrefix<'_>);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = Ib {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			start: 42,
		};
		let enc = Ib::encode_key(&val).unwrap();
		assert_eq!(
			enc.as_slice(),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!ib\x80\0\0\0\0\0\0\x2A"
		);
	}
}
