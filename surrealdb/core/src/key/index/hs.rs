//! Store state of an HNSW index
use std::borrow::Cow;
use std::fmt::Debug;

use surrealdb_strand::TableName;

use crate::catalog::IndexId;
use crate::idx::trees::hnsw::HnswState;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};

key! {
	#[derive(Debug, Clone, PartialEq)]
	pub(crate) struct Hs<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'h',
		b's',
	}
}

impl_kv_key_storekey!(Hs<'a> => HnswState);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = Hs {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
		};
		let enc = Hs::encode_key(&val).unwrap();
		assert_eq!(
			enc.as_slice(),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hs",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
