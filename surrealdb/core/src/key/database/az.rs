//! Stores a DEFINE ANALYZER config definition
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, impl_kv_range_storekey, key};

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Analyzer<'a> {
		pub prefix: DatabaseRoot,
		b'!',
		b'a',
		b'z',
		pub az: Cow<'a, str>,
	}
}
impl_kv_key_storekey!(Analyzer<'a> => catalog::AnalyzerDefinition);
impl Categorise for Analyzer<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseAnalyzer
	}
}

key! {
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct AnalyzerPrefix {
		pub prefix: DatabaseRoot,
		b'!',
		b'a',
		b'z',
	}
}
impl_kv_range_storekey!(AnalyzerPrefix);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn key() {
		let val = Analyzer {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			az: "test".into(),
		};
		let enc = Analyzer::encode_key(&val).unwrap();
		assert_eq!(enc.as_slice(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!aztest\0");
	}

	#[test]
	fn prefix() {
		let val = AnalyzerPrefix {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
		};
		let val = storekey::encode_vec(&val).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!az");
	}
}
