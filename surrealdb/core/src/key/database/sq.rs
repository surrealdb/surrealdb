//! Stores a DEFINE SEQUENCE config definition
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::SequenceDefinition;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Sq<'a> {
		pub prefix: DatabaseRoot,
		b'*', // *
		b's', // s
		b'q', // q
		pub sq: Cow<'a, str>,
	}
}

impl_kv_key_storekey!(Sq<'a> => SequenceDefinition);
impl Categorise for Sq<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseSequence
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct SqPrefix {
		pub prefix: DatabaseRoot,
		b'*', // *
		b's', // s
		b'q', // q
	}
}
impl_kv_range_storekey!(SqPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::{KVKey, KVRange};

	#[test]
	fn key() {
		let val = Sq {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			sq: "test".into(),
		};
		let enc = Sq::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*sqtest\0");
	}

	#[test]
	fn prefix() {
		let val = SqPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
		}
		.encode_range()
		.unwrap();
		assert_eq!(val.start.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*sq\0");
	}
}
