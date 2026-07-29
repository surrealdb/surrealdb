//! Stores a DEFINE CONFIG definition
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::StoredConfigDefinition;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Config<'a> {
		pub prefix: DatabaseRoot,
		b'!',
		b'c',
		b'g',
		pub ty: Cow<'a, str>,
	}
}
impl_kv_key_storekey!(Config<'a> => StoredConfigDefinition);
impl Categorise for Config<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseConfig
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct ConfigPrefix {
		pub prefix: DatabaseRoot,
		b'!',
		b'c',
		b'g',
	}
}
impl_kv_range_storekey!(ConfigPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = Config {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			ty: "testty".into(),
		};
		let enc = Config::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!cgtestty\0");
	}

	#[test]
	fn test_prefix() {
		let val = ConfigPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
		};
		let val = storekey::encode_vec(&val).unwrap();
		assert_eq!(&val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!cg");
	}
}
