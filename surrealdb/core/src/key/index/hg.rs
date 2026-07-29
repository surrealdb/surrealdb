//! HNSW compaction generation key.

use std::borrow::Cow;

use surrealdb_strand::TableName;

use crate::catalog::IndexId;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};

key! {
		/// HNSW pending compaction generation.
		///
		/// The generation lets compactors validate that the pending snapshot they
		/// gathered is still current before deleting exact pending keys and mutating
		/// the graph. Missing values are treated as generation `0`.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
		pub(crate) struct Hg<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'h',
		b'g',
	}
}
impl_kv_key_storekey!(Hg<'a> => u64);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = Hg {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
		};
		let enc = Hg::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hg");
	}
}
