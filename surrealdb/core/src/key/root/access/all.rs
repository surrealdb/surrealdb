//! Stores the key prefix for all keys under a root access method
use std::borrow::Cow;

use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct AccessRoot<'a> {
		b'/',
		b'&',
		pub ac: Cow<'a, str>,
	}
}

impl_kv_range_storekey!(AccessRoot<'_>);

impl Categorise for AccessRoot<'_> {
	fn categorise(&self) -> Category {
		Category::AccessRoot
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVRange;

	#[test]
	fn key() {
		let val = AccessRoot {
			ac: "testac".into(),
		};
		let enc = AccessRoot::encode_bound(&val).unwrap();
		assert_eq!(&*enc, b"/&testac\0");
	}
}
