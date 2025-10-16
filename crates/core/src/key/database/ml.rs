//! Stores a DEFINE MODEL config definition
use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, MlModelDefinition, NamespaceId};
use crate::kvs::{KVKey, impl_kv_key_storekey};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct MlModelDefinitionKey<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
	pub ml: Cow<'a, str>,
	pub vn: Cow<'a, str>,
}

impl_kv_key_storekey!(MlModelDefinitionKey<'_> => MlModelDefinition);

pub fn new<'a>(
	ns: NamespaceId,
	db: DatabaseId,
	ml: &'a str,
	vn: &'a str,
) -> MlModelDefinitionKey<'a> {
	MlModelDefinitionKey::new(ns, db, ml, vn)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!ml\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"!ml\xff");
	Ok(k)
}

impl<'a> MlModelDefinitionKey<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, ml: &'a str, vn: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'!',
			_d: b'm',
			_e: b'l',
			ml: Cow::Borrowed(ml),
			vn: Cow::Borrowed(vn),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = MlModelDefinitionKey::new(
			NamespaceId(1),
			DatabaseId(2),
			"testml",
			"1.0.0",
		);
		let enc = MlModelDefinitionKey::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!mltestml\x001.0.0\0");
	}

	#[test]
	fn test_prefix() {
		let val = super::prefix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ml\0");
	}

	#[test]
	fn test_suffix() {
		let val = super::suffix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!ml\xff");
	}
}
