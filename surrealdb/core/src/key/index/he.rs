//! Stores Vector of an HNSW index
use std::borrow::Cow;

use surrealdb_strand::TableName;

use crate::catalog::IndexId;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::vector::SerializedVector;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct He<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'h',
		b'e',
		pub element_id: ElementId,
	}
}

impl_kv_key_storekey!(He<'a> => SerializedVector);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = He {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			element_id: 7,
		};

		let enc = He::encode_key(&val).unwrap();
		assert_eq!(
			&*enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!he\0\0\0\0\0\0\0\x07"
		);
	}
}
