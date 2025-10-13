//! Stores a graph edge pointer
use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::{KVKey, impl_kv_key_storekey};
use crate::val::RecordIdKey;

#[derive(Clone, Debug, Eq, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "()")]
struct Prefix<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
	_d: u8,
	pub id: RecordIdKey,
}

impl_kv_key_storekey!(Prefix<'_> => Vec<u8>);

impl<'a> Prefix<'a> {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, id: &RecordIdKey) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'&',
			id: id.clone(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "()")]
struct PrefixFt<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
	_d: u8,
	pub id: RecordIdKey,
	pub ft: Cow<'a, str>,
}

impl_kv_key_storekey!(PrefixFt<'_> => Vec<u8>);

// Code here is used in references which is temporarly disabled
#[allow(dead_code)]
impl<'a> PrefixFt<'a> {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, id: &RecordIdKey, ft: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'&',
			id: id.clone(),
			ft: Cow::Borrowed(ft),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "()")]
struct PrefixFk<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
	_d: u8,
	pub id: RecordIdKey,
	pub ft: Cow<'a, str>,
	pub fk: RecordIdKey,
}

impl_kv_key_storekey!(PrefixFk<'_> => Vec<u8>);

// Code here is used in references which is temporarly removed
#[allow(dead_code)]
impl<'a> PrefixFk<'a> {
	fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		id: &RecordIdKey,
		ft: &'a str,
		fk: &RecordIdKey,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'&',
			id: id.clone(),
			ft: Cow::Borrowed(ft),
			fk: fk.clone(),
		}
	}
}

// The order in this key is made so we can scan:
// - all references for a given record
// - all references for a given record, filtered by a origin table
// - all references for a given record, filtered by a origin table and an origin field

#[derive(Clone, Debug, Eq, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Ref<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
	_d: u8,
	pub id: Cow<'a, RecordIdKey>,
	pub ft: Cow<'a, str>,
	pub fk: Cow<'a, RecordIdKey>,
	pub ff: Cow<'a, str>,
}

impl_kv_key_storekey!(Ref<'_> => ());

impl Ref<'_> {
	pub fn decode_key(k: &[u8]) -> Result<Ref<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}
}

pub fn new<'a>(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &'a str,
	id: &'a RecordIdKey,
	ft: &'a str,
	fk: &'a RecordIdKey,
	ff: &'a str,
) -> Ref<'a> {
	Ref::new(ns, db, tb, id, ft, fk, ff)
}

pub fn prefix(ns: NamespaceId, db: DatabaseId, tb: &str, id: &RecordIdKey) -> Result<Vec<u8>> {
	let mut k = Prefix::new(ns, db, tb, id).encode_key()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId, tb: &str, id: &RecordIdKey) -> Result<Vec<u8>> {
	let mut k = Prefix::new(ns, db, tb, id).encode_key()?;
	k.extend_from_slice(&[0xff]);
	Ok(k)
}

pub fn ftprefix(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &str,
	id: &RecordIdKey,
	ft: &str,
) -> Result<Vec<u8>> {
	let mut k = PrefixFt::new(ns, db, tb, id, ft).encode_key()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

pub fn ftsuffix(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &str,
	id: &RecordIdKey,
	ft: &str,
) -> Result<Vec<u8>> {
	let mut k = PrefixFt::new(ns, db, tb, id, ft).encode_key()?;
	k.extend_from_slice(&[0xff]);
	Ok(k)
}

pub fn fkprefix(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &str,
	id: &RecordIdKey,
	ft: &str,
	fk: &RecordIdKey,
) -> Result<Vec<u8>> {
	let mut k = PrefixFk::new(ns, db, tb, id, ft, fk).encode_key()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

pub fn fksuffix(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &str,
	id: &RecordIdKey,
	ft: &str,
	fk: &RecordIdKey,
) -> Result<Vec<u8>> {
	let mut k = PrefixFk::new(ns, db, tb, id, ft, fk).encode_key()?;
	k.extend_from_slice(&[0xff]);
	Ok(k)
}

impl Categorise for Ref<'_> {
	fn categorise(&self) -> Category {
		Category::Ref
	}
}

impl<'a> Ref<'a> {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		id: &'a RecordIdKey,
		ft: &'a str,
		fk: &'a RecordIdKey,
		ff: &'a str,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'&',
			id: Cow::Borrowed(id),
			ft: Cow::Borrowed(ft),
			fk: Cow::Borrowed(fk),
			ff: Cow::Borrowed(ff),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let binding = RecordIdKey::String("testid".into());
		let other_binding = RecordIdKey::String("otherid".into());
		let val = Ref::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			&binding,
			"othertb",
			&other_binding,
			"test.*",
		);
		let enc = Ref::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00&\x03testid\0othertb\0\x03otherid\0test.*\0"
		);
	}
}
