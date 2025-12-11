//! Stores a graph edge pointer
use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::expr::dir::Dir;
use crate::key::category::{Categorise, Category};
use crate::kvs::{KVKey, impl_kv_key_storekey};
use crate::val::{RecordId, RecordIdKey};

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
			_d: b'~',
			id: id.to_owned(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "()")]
struct PrefixEg<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
	_d: u8,
	pub id: RecordIdKey,
	pub eg: Dir,
}

impl_kv_key_storekey!(PrefixEg<'_> => Vec<u8>);

impl<'a> PrefixEg<'a> {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, id: &RecordIdKey, eg: &Dir) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'~',
			id: id.clone(),
			eg: eg.clone(),
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
	pub eg: Dir,
	pub ft: Cow<'a, str>,
}

impl_kv_key_storekey!(PrefixFt<'_> => Vec<u8>);

impl<'a> PrefixFt<'a> {
	fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		id: &RecordIdKey,
		eg: &Dir,
		ft: &'a str,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'~',
			id: id.to_owned(),
			eg: eg.to_owned(),
			ft: Cow::Borrowed(ft),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Graph<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
	_d: u8,
	pub id: RecordIdKey,
	pub eg: Dir,
	pub ft: Cow<'a, str>,
	pub fk: Cow<'a, RecordIdKey>,
}

impl_kv_key_storekey!(Graph<'_> => ());

impl Graph<'_> {
	pub fn decode_key(k: &[u8]) -> Result<Graph<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}
}

pub fn new<'a>(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &'a str,
	id: &RecordIdKey,
	eg: &Dir,
	fk: &'a RecordId,
) -> Graph<'a> {
	Graph::new(ns, db, tb, id.to_owned(), eg.to_owned(), fk)
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

pub fn egprefix(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &str,
	id: &RecordIdKey,
	eg: &Dir,
) -> Result<Vec<u8>> {
	let mut k = PrefixEg::new(ns, db, tb, id, eg).encode_key()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

pub fn egsuffix(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &str,
	id: &RecordIdKey,
	eg: &Dir,
) -> Result<Vec<u8>> {
	let mut k = PrefixEg::new(ns, db, tb, id, eg).encode_key()?;
	k.extend_from_slice(&[0xff]);
	Ok(k)
}

pub fn ftprefix(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &str,
	id: &RecordIdKey,
	eg: &Dir,
	ft: &str,
) -> Result<Vec<u8>> {
	let mut k = PrefixFt::new(ns, db, tb, id, eg, ft).encode_key()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

pub fn ftsuffix(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &str,
	id: &RecordIdKey,
	eg: &Dir,
	ft: &str,
) -> Result<Vec<u8>> {
	let mut k = PrefixFt::new(ns, db, tb, id, eg, ft).encode_key()?;
	k.extend_from_slice(&[0xff]);
	Ok(k)
}

impl Categorise for Graph<'_> {
	fn categorise(&self) -> Category {
		Category::Graph
	}
}

impl<'a> Graph<'a> {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		id: RecordIdKey,
		eg: Dir,
		fk: &'a RecordId,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'~',
			id,
			eg,
			ft: Cow::Borrowed(&fk.table),
			fk: Cow::Borrowed(&fk.key),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::syn;
	use crate::types::PublicValue;

	#[test]
	fn key() {
		let Ok(PublicValue::RecordId(fk)) = syn::value("other:test") else {
			panic!()
		};
		let fk = fk.into();
		let val = Graph::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testid".to_owned().into(),
			Dir::Out,
			&fk,
		);
		let enc = Graph::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0~\x03testid\0\x03other\0\x03test\0"
		);
	}
}
