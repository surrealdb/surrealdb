//! Stores the next and available freed IDs for documents
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::idg::u32::U32;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::kvs::KVKey;

// Index ID generator
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct IndexIdGeneratorKey {
	table_root: DatabaseRoot,
	_c: u8,
	_d: u8,
	_e: u8,
}

impl KVKey for IndexIdGeneratorKey {
	type ValueType = U32;
}

pub fn new(ns: NamespaceId, db: DatabaseId) -> IndexIdGeneratorKey {
	IndexIdGeneratorKey::new(ns, db)
}

impl Categorise for IndexIdGeneratorKey {
	fn categorise(&self) -> Category {
		Category::DatabaseTableIdentifier
	}
}

impl IndexIdGeneratorKey {
	pub fn new(ns: NamespaceId, db: DatabaseId) -> Self {
		IndexIdGeneratorKey {
			table_root: DatabaseRoot::new(ns, db),
			_c: b'!',
			_d: b't',
			_e: b'i',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = IndexIdGeneratorKey::new(
			NamespaceId(123),
			DatabaseId(234),
		);
		let enc = IndexIdGeneratorKey::encode_key(&val).unwrap();
		assert_eq!(&enc, b"/*\x00\x00\x00\x7b*\x00\x00\x00\xea!\x74\x69");
	}
}
