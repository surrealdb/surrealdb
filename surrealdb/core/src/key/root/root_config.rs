//! Stores a DEFINE CONFIG definition
use std::borrow::Cow;

use crate::catalog::StoredConfigDefinition;
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct RootConfig<'a> {
		b'/',
		b'!',
		b'c',
		b'g',
		pub ty: Cow<'a, str>,
	}
}

impl_kv_key_storekey!(RootConfig<'a> => StoredConfigDefinition);

impl Categorise for RootConfig<'_> {
	fn categorise(&self) -> Category {
		Category::RootConfig
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = RootConfig {
			ty: "testty".into(),
		};
		let enc = RootConfig::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/!cgtestty\0");
	}
}
