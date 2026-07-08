//! Stores the key prefix for all keys under a database access method
use std::borrow::Cow;

use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct DbAccess<'a> {
		pub prefix: DatabaseRoot,
		b'&',
		pub ac: Cow<'a, str>,
	}
}
impl_kv_range_storekey!(DbAccess<'_>);

impl Categorise for DbAccess<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseAccessRoot
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVRange;

	#[test]
	fn key() {
		let val = DbAccess {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			ac: "testac".into(),
		};
		let enc = DbAccess::encode_bound(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02&testac\0");
	}
}
