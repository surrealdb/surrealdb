//! Stores cluster membership information
use uuid::Uuid;

use crate::dbs::node::Node;
use crate::key::category::{Categorise, Category};
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	// Represents cluster information.
	// In the future, this could also include broadcast addresses and other
	// information.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Nd {
		b'/',
		b'!',
		b'n',
		b'd',
		pub nd: Uuid,
	}
}
impl_kv_key_storekey!(Nd => Node);

impl Categorise for Nd {
	fn categorise(&self) -> Category {
		Category::Node
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct NdPrefix {
		b'/',
		b'!',
		b'n',
		b'd',
	}
}
impl_kv_range_storekey!(NdPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let val = Nd {
			nd: Uuid::default(),
		};
		let enc = val.encode_key().unwrap();
		assert_eq!(&*enc, b"/!nd\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00");
	}

	#[test]
	fn test_prefix() {
		let val = NdPrefix {}.encode_range().unwrap();
		assert_eq!(val.start.as_slice(), b"/!nd\0");
		assert_eq!(val.end.as_slice(), b"/!ne");
	}
}
