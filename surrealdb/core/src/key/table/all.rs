//! Stores the key prefix for all keys under a table
use std::borrow::Cow;

use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_range_storekey, key};
use crate::val::TableName;

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct TableRoot<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
	}
}

impl_kv_range_storekey!(TableRoot<'_>);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVRange;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let val = TableRoot {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
		};
		let enc = TableRoot::encode_bound(&val).unwrap();
		assert_eq!(&*enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0");
	}
}
