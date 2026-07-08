//! Stores a record document

use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Default)]
	pub(crate) struct Version {
		b'!',
		b'v',
	}
}
impl_kv_range_storekey!(Version);
impl_kv_key_storekey!(Version => crate::kvs::version::MajorVersion);

impl Categorise for Version {
	fn categorise(&self) -> Category {
		Category::Version
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = Version {};
		let enc = Version::encode_key(&val).unwrap();
		assert_eq!(&*enc, b"!v");
	}
}
