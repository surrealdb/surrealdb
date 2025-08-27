//! Stores the next and available freed IDs for documents
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::kvs::KVKey;

// Table ID generator
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Ti {
	table_root: DatabaseRoot,
	_c: u8,
	_d: u8,
	_e: u8,
}

impl KVKey for Ti {
	type ValueType = Vec<u8>;
}

pub fn new(ns: NamespaceId, db: DatabaseId) -> Ti {
	Ti::new(ns, db)
}

impl Categorise for Ti {
	fn categorise(&self) -> Category {
		Category::DatabaseTableIdentifier
	}
}

impl Ti {
	pub fn new(ns: NamespaceId, db: DatabaseId) -> Self {
		Ti {
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
		let val = Ti::new(
			NamespaceId(123),
			DatabaseId(234),
		);
		let enc = Ti::encode_key(&val).unwrap();
		assert_eq!(&enc, b"/*\x00\x00\x00\x7b*\x00\x00\x00\xea!\x74\x69");
	}
}
