//! Stores a DEFINE FUNCTION config definition
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::StoredFunctionDefinition;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Fc<'a> {
		pub prefix: DatabaseRoot,
		b'!',
		b'f',
		b'n',
		pub fc: Cow<'a, str>,
	}
}
impl_kv_key_storekey!(Fc<'a> => StoredFunctionDefinition);
impl Categorise for Fc<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseFunction
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct FcPrefix {
		pub prefix: DatabaseRoot,
		b'!',
		b'f',
		b'n',
	}
}
impl_kv_range_storekey!(FcPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = Fc {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			fc: "testfc".into(),
		};
		let enc = Fc::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!fntestfc\0");
	}
}
