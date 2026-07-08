//! Index Sequence State Key Structure
//!
//! This module defines the `Is` key structure used to store distributed
//! sequence states for full-text search document ID generation. The key enables
//! concurrent indexing by maintaining a sequence state per node in a
//! distributed system.
//!
//! # Purpose
//!
//! The `Is` key stores the state of distributed sequences used to provide
//! unique numeric IDs to documents during full-text indexing operations. This
//! allows multiple nodes to concurrently index documents while maintaining
//! unique document identifiers.
//!
//! # Key Structure
//!
//! The key follows the pattern: `/*{ns}*{db}*{tb}+{ix}!ib{nid}`
//!
//! Where:
//! - `ns`: Namespace identifier
//! - `db`: Database identifier
//! - `tb`: Table identifier
//! - `ix`: Index identifier
//! - `nid`: Node UUID (16 bytes, compact serialized)
use std::borrow::Cow;

use uuid::Uuid;

use crate::catalog::IndexId;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};
use crate::kvs::sequences::SequenceState;
use crate::val::TableName;

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Is<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'i',
		b's',
		pub nid: Uuid,
	}
}

impl_kv_key_storekey!(Is<'a> => SequenceState);

impl Categorise for Is<'_> {
	fn categorise(&self) -> Category {
		Category::IndexFullTextDocIdsSequenceState
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = Is {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			nid: Uuid::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
		};
		let enc = Is::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!is\0\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f");
	}
}
