//! Stores a DEFINE TABLE config definition
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::TableDefinition;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::val::TableName;

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct TableKey<'a> {
		pub prefix: DatabaseRoot,
		b'!',
		b't',
		b'b',
		pub tb: Cow<'a, TableName>,
	}
}
impl_kv_key_storekey!(TableKey<'a> => TableDefinition);
impl Categorise for TableKey<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseTable
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct TableKeyPrefix {
		pub prefix: DatabaseRoot,
		b'!',
		b't',
		b'b',
	}
}
impl_kv_range_storekey!(TableKeyPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = TableKey {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
		};
		let enc = TableKey::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!tbtesttb\0");
	}
}
