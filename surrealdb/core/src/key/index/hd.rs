//! Stores the DocIds -> Thing of an HNSW index
use std::borrow::Cow;

use crate::catalog::IndexId;
use crate::idx::seqdocids::DocId;
use crate::idx::trees::hnsw::docs::HnswDocsState;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};
use crate::val::{RecordIdKey, TableName};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Hd<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'h',
		b'd',
		pub doc_id: DocId,
	}
}
impl_kv_key_storekey!(Hd<'a> => RecordIdKey);

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct HdPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'h',
		b'd',
	}
}

impl_kv_key_storekey!(HdPrefix<'a> => HnswDocsState);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn root() {
		let tb = TableName::from("testtb");
		let val = HdPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
		};
		let enc = HdPrefix::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hd");
	}

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = Hd {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			doc_id: 7,
		};
		let enc = Hd::encode_key(&val).unwrap();
		assert_eq!(
			&*enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hd\0\0\0\0\0\0\0\x07"
		);
	}
}
