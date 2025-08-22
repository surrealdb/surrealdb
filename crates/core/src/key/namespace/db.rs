//! Stores a DEFINE DATABASE config definition
use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseDefinition, DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct NsDbRoot {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	_c: u8,
	_d: u8,
	pub db: DatabaseId,
}

impl KVKey for NsDbRoot {
	type ValueType = DatabaseDefinition;
}

pub fn new(ns: NamespaceId, db: DatabaseId) -> NsDbRoot {
	NsDbRoot::new(ns, db)
}

pub fn prefix(ns: NamespaceId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns).encode_key()?;
	k.extend_from_slice(b"!db\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns).encode_key()?;
	k.extend_from_slice(b"!db\xff");
	Ok(k)
}

impl Categorise for NsDbRoot {
	fn categorise(&self) -> Category {
		Category::DatabaseAlias
	}
}

impl NsDbRoot {
	pub fn new(ns: NamespaceId, db: DatabaseId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'!',
			_c: b'd',
			_d: b'b',
			db,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = NsDbRoot::new(
			NamespaceId(1),
			DatabaseId(2),
		);
		let enc = NsDbRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01!db\x00\x00\x00\x02");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(NamespaceId(1)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01!db\0")
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(NamespaceId(1)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01!db\xff")
	}
}
