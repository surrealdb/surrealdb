//! Stores a DEFINE PARAM config definition
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::ParamDefinition;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Pa<'a> {
		pub prefix: DatabaseRoot,
		 b'!',
		 b'p',
		 b'a',
		pub pa: Cow<'a, str>,
	}
}

impl_kv_key_storekey!(Pa<'a> => ParamDefinition);

impl Categorise for Pa<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseParameter
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct PaPrefix {
		pub prefix: DatabaseRoot,
		 b'!',
		 b'p',
		 b'a',
	}
}
impl_kv_range_storekey!(PaPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = Pa {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			pa: Cow::Borrowed("testpa"),
		};
		let enc = Pa::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!patestpa\0");
	}
}
