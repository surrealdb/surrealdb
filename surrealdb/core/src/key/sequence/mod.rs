//! Stores sequence states
pub mod ba;
pub mod st;

use std::borrow::Cow;

use anyhow::Result;

use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct BaPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'!',
		b's',
		b'q',
		pub sq: Cow<'a, str>,
		b'!',
		b'b',
		b'a',
	}
}
impl_kv_range_storekey!(BaPrefix<'_>);

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct StPrefix<'a> {
		pub prefix: DatabaseRoot,
		b'!',
		b's',
		b'q',
		pub sq: Cow<'a, str>,
		b'!',
		b's',
		b't',
	}
}

impl_kv_range_storekey!(StPrefix<'_>);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVRange;

	#[test]
	fn ba_range() {
		let range = BaPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			sq: "testsq".into(),
		}
		.encode_range()
		.unwrap();
		assert_eq!(range.start.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!sqtestsq\0!ba\0");
		assert_eq!(range.end.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!sqtestsq\0!bb");
	}
}
