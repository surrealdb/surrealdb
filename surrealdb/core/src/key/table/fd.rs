//! Stores a DEFINE FIELD config definition
use std::borrow::Cow;

use anyhow::Result;
use surrealdb_strand::TableName;

use crate::catalog::{self};
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Fd<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'f',
		b'd',
		pub fd: Cow<'a, str>,
	}
}

impl_kv_key_storekey!(Fd<'a> => catalog::StoredFieldDefinition);

impl Categorise for Fd<'_> {
	fn categorise(&self) -> Category {
		Category::TableField
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct FdPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'f',
		b'd',
	}
}

impl_kv_range_storekey!(FdPrefix<'_>);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = Fd {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			fd: "testfd".into(),
		};
		let enc = Fd::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fdtestfd\0");
	}

	#[test]
	fn test_prefix() {
		let tb = TableName::from("testtb");
		let val = FdPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
		}
		.encode_range()
		.unwrap();
		assert_eq!(val.start.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fd\0");
		assert_eq!(val.end.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!fe");
	}
}
