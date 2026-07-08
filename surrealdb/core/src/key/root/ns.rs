//! Stores a DEFINE NAMESPACE config definition
use std::borrow::Cow;

use crate::catalog::NamespaceDefinition;
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd,)]
	pub(crate) struct NamespaceKey<'key> {
		b'/',
		b'!',
		b'n',
		b's',
		pub ns: Cow<'key, str>,
	}
}

impl_kv_key_storekey!(NamespaceKey<'a> => NamespaceDefinition);
impl Categorise for NamespaceKey<'_> {
	fn categorise(&self) -> Category {
		Category::Namespace
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct NamespacePrefix {
		b'/',
		b'!',
		b'n',
		b's',
	}
}
impl_kv_range_storekey!(NamespacePrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = NamespaceKey {
			ns: Cow::Borrowed("test"),
		};
		let enc = NamespaceKey::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"/!nstest\0");
	}
}
