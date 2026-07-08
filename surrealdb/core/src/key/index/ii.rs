//! Stores doc keys for doc_ids
use std::borrow::Cow;

use crate::catalog::IndexId;
use crate::idx::seqdocids::DocId;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};
use crate::val::{RecordIdKey, TableName};

key! {
/// Id inverted. DocId -> Id
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
		pub(crate) struct Ii<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'i',
		b'i',
		pub id: DocId,
	}
}

impl_kv_key_storekey!(Ii<'a> => RecordIdKey);

impl Categorise for Ii<'_> {
	fn categorise(&self) -> Category {
		Category::IndexDocKeys
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
		let val = Ii {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			id: 1,
		};
		let enc = Ii::encode_key(&val).unwrap();
		assert_eq!(
			enc.as_slice(),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!ii\0\0\0\0\0\0\0\x01"
		);
	}
}
