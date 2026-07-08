//! Stores the key prefix for all keys under an index
use std::borrow::Cow;

use crate::catalog::IndexId;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_range_storekey, key};
use crate::val::TableName;

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct AllIndexRoot<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
	}
}
//impl_kv_key_storekey!(AllIndexRoot<'a> => Vec<u8>);
impl_kv_range_storekey!(AllIndexRoot<'_>);

impl Categorise for AllIndexRoot<'_> {
	fn categorise(&self) -> Category {
		Category::IndexRoot
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVRange;

	#[test]
	fn root() {
		let tb = TableName::from("testtb");
		let val = AllIndexRoot {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
		};
		let enc = AllIndexRoot::encode_bound(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03");
	}
}
