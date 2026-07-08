//! Stores the key prefix for all keys under a namespace access method
use std::borrow::Cow;

use crate::catalog::NamespaceId;
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_range_storekey, key};
key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct AccessRoot<'a> {
		b'/',
		b'*',
		pub ns: NamespaceId,
		b'&',
		pub ac: Cow<'a, str>,
	}
}

impl_kv_range_storekey!(AccessRoot<'_>);

impl Categorise for AccessRoot<'_> {
	fn categorise(&self) -> Category {
		Category::NamespaceAccessRoot
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVRange;

	#[test]
	fn key() {
		let val = AccessRoot {
			ns: NamespaceId(1),
			ac: "testac".into(),
		};
		let enc = AccessRoot::encode_bound(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01&testac\0");
	}
}
