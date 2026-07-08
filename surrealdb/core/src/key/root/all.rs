//! Stores the key prefix for all keys

use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Default)]
	pub(crate) struct Kv {
		b'/',
	}
}

impl_kv_range_storekey!(Kv);

impl Categorise for Kv {
	fn categorise(&self) -> Category {
		Category::Root
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVRange;

	#[test]
	fn key() {
		let val = Kv {};
		let enc = Kv::encode_range(&val).unwrap();
		assert_eq!(enc.start.as_slice(), b"/\0");
	}
}
