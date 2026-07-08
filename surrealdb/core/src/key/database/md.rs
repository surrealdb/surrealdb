//! Stores a DEFINE FUNCTION config definition
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::ModuleDefinition;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Md<'a> {
		pub prefix: DatabaseRoot,
			b'!',
			b'm',
			b'd',
		pub md: Cow<'a, str>,
	}
}
impl_kv_key_storekey!(Md<'a> => ModuleDefinition);

impl Categorise for Md<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseFunction
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct MdPrefix {
		pub prefix: DatabaseRoot,
		b'!',
		b'm',
		b'd',
	}
}
impl_kv_range_storekey!(MdPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = Md {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			md: "testmd".into(),
		};
		let enc = Md::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!mdtestmd\0");
	}
}
