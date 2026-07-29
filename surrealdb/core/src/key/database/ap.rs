//! Stores a DEFINE API definition
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::{DatabaseId, NamespaceId, StoredApiDefinition};
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Api<'a> {
		pub prefix: DatabaseRoot,
		b'!',
		b'a',
		b'p',
		pub ap: Cow<'a, str>,
	}
}

impl_kv_key_storekey!(Api<'a> => StoredApiDefinition);
impl Categorise for Api<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseApi
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct ApiPrefix {
		b'/',
		b'*',
		pub ns: NamespaceId,
		b'*',
		pub db: DatabaseId,
		b'!',
		b'a',
		b'p',
	}
}
impl_kv_range_storekey!(ApiPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = Api {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			ap: "test".into(),
		};
		let enc = Api::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!aptest\0");
	}

	#[test]
	fn prefix() {
		let val = ApiPrefix {
			ns: NamespaceId(1),
			db: DatabaseId(2),
		};
		let enc = storekey::encode_vec(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ap");
	}
}
