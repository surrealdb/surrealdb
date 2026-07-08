//! Stores sequence batches
use std::borrow::Cow;

use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};
use crate::kvs::sequences::BatchValue;

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Ba<'a> {
		pub prefix: DatabaseRoot,
		b'!',
		b's',
		b'q',
		pub sq: Cow<'a, str>,
		b'!',
		b'b',
		b'a',
		pub start: i64,
	}
}

impl_kv_key_storekey!(Ba<'a> => BatchValue);

impl Categorise for Ba<'_> {
	fn categorise(&self) -> Category {
		Category::SequenceBatch
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = Ba {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			sq: "testsq".into(),
			start: 100,
		};

		let enc = Ba::encode_key(&val).unwrap();
		assert_eq!(
			enc.as_slice(),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!sqtestsq\0!ba\x80\0\0\0\0\0\0\x64"
		);
	}
}
