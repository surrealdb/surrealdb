//! Store chunk data for layers of an HNSW index
use std::borrow::Cow;
use std::fmt::Debug;

use surrealdb_strand::TableName;

use crate::catalog::IndexId;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Debug, Clone, PartialEq)]
	pub(crate) struct Hl<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'h',
		b'l',
		pub layer: u16,
		pub chunk: u32,
	}
}

impl_kv_key_storekey!(Hl<'a> => Vec<u8>);

key! {
	#[derive(Debug, Clone, PartialEq)]
	pub(crate) struct HlPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'h',
		b'l',
		pub layer: u16,
	}
}
impl_kv_range_storekey!(HlPrefix<'_>);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = Hl {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			layer: 7,
			chunk: 8,
		};
		let enc = Hl::encode_key(&val).unwrap();
		assert_eq!(
			enc.as_slice(),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hl\0\x07\0\0\0\x08",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
