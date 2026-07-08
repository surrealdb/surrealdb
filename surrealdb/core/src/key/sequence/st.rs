//! Stores sequence states
use std::borrow::Cow;

use uuid::Uuid;

use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};
use crate::kvs::sequences::SequenceState;

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct St<'a> {
		pub root: DatabaseRoot,
		b'!',
		b's',
		b'q',
		pub sq: Cow<'a, str>,
		b'!',
		b's',
		b't',
		pub nid: Uuid,
	}
}

impl_kv_key_storekey!(St<'a> => SequenceState);

impl Categorise for St<'_> {
	fn categorise(&self) -> Category {
		Category::SequenceState
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = St {
			root: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			sq: Cow::Borrowed("testsq"),
			nid: Uuid::from_bytes([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
		};
		let enc = St::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!sqtestsq\0!st\0\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f");
	}
}
