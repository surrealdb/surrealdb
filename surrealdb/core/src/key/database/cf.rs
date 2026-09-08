//! Stores a database's changefeed-retention fence.
//!
//! The value is a counter with no meaning beyond changing. It exists to give
//! the changefeed collector and the statements that write a retention a key in
//! common: a collection page arms against it, and a retention write bumps it,
//! so a page that armed before the write cannot commit after it. Without that
//! shared key the two touch disjoint keyspaces and never conflict, and a
//! retention extended mid-pass would let the page delete entries the new
//! policy keeps — which a changefeed delete cannot undo.

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Cf {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
}

impl_kv_key_storekey!(Cf => u64);

pub fn new(ns: NamespaceId, db: DatabaseId) -> Cf {
	Cf::new(ns, db)
}

impl Categorise for Cf {
	fn categorise(&self) -> Category {
		Category::DatabaseChangefeedFence
	}
}

impl Cf {
	pub fn new(ns: NamespaceId, db: DatabaseId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'c',
			_e: b'f',
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		let val = Cf::new(NamespaceId(1), DatabaseId(2));
		let enc = Cf::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!cf");
	}
}
