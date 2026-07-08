//! Stores a DEFINE TABLE AS config definition
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::TableDefinition;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};
use crate::val::TableName;

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Ft<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'f',
		b't',
		pub ft: Cow<'a, TableName>,
	}
}

impl_kv_key_storekey!(Ft<'a> => TableDefinition);

impl Categorise for Ft<'_> {
	fn categorise(&self) -> Category {
		Category::TableView
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct FtPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'f',
		b't',
	}
}
impl_kv_range_storekey!(FtPrefix<'_>);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let ft = TableName::from("testft");
		let val = Ft {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ft: Cow::Borrowed(&ft),
		};
		let enc = Ft::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fttestft\0");
	}

	#[test]
	fn test_prefix() {
		let tb = TableName::from("testtb");
		let range = FtPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
		}
		.encode_range()
		.unwrap();
		assert_eq!(range.start.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!ft\0");
		assert_eq!(range.end.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fu");
	}
}
