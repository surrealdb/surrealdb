//! Stores a graph edge pointer
use crate::catalog::DatabaseId;
use crate::catalog::NamespaceId;
use crate::expr::id::Id;
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::KVKey;

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
struct Prefix<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: Id,
}

impl KVKey for Prefix<'_> {
	type ValueType = Vec<u8>;
}

impl<'a> Prefix<'a> {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, id: &Id) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'&',
			id: id.to_owned(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
struct PrefixFt<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: Id,
	pub ft: &'a str,
}

impl KVKey for PrefixFt<'_> {
	type ValueType = Vec<u8>;
}

impl<'a> PrefixFt<'a> {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, id: &Id, ft: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'&',
			id: id.to_owned(),
			ft,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
struct PrefixFf<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: Id,
	pub ft: &'a str,
	pub ff: &'a str,
}

impl KVKey for PrefixFf<'_> {
	type ValueType = Vec<u8>;
}

impl<'a> PrefixFf<'a> {
	fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		id: &Id,
		ft: &'a str,
		ff: &'a str,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'&',
			id: id.to_owned(),
			ft,
			ff,
		}
	}
}

// The order in this key is made so we can scan:
// - all references for a given record
// - all references for a given record, filtered by a origin table
// - all references for a given record, filtered by a origin table and an origin field

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Ref<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: Id,
	pub ft: &'a str,
	pub ff: &'a str,
	pub fk: Id,
}

impl KVKey for Ref<'_> {
	type ValueType = ();
}

impl Ref<'_> {
	pub fn decode_key(k: &[u8]) -> Result<Ref<'_>> {
		Ok(storekey::deserialize(k)?)
	}
}

pub fn new<'a>(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &'a str,
	id: &Id,
	ft: &'a str,
	ff: &'a str,
	fk: &Id,
) -> Ref<'a> {
	Ref::new(ns, db, tb, id.to_owned(), ft, ff, fk.to_owned())
}

pub fn prefix(ns: NamespaceId, db: DatabaseId, tb: &str, id: &Id) -> Result<Vec<u8>> {
	let mut k = Prefix::new(ns, db, tb, id).encode_key()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId, tb: &str, id: &Id) -> Result<Vec<u8>> {
	let mut k = Prefix::new(ns, db, tb, id).encode_key()?;
	k.extend_from_slice(&[0xff]);
	Ok(k)
}

pub fn ftprefix(ns: NamespaceId, db: DatabaseId, tb: &str, id: &Id, ft: &str) -> Result<Vec<u8>> {
	let mut k = PrefixFt::new(ns, db, tb, id, ft).encode_key()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

pub fn ftsuffix(ns: NamespaceId, db: DatabaseId, tb: &str, id: &Id, ft: &str) -> Result<Vec<u8>> {
	let mut k = PrefixFt::new(ns, db, tb, id, ft).encode_key()?;
	k.extend_from_slice(&[0xff]);
	Ok(k)
}

pub fn ffprefix(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &str,
	id: &Id,
	ft: &str,
	ff: &str,
) -> Result<Vec<u8>> {
	let mut k = PrefixFf::new(ns, db, tb, id, ft, ff).encode_key()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

pub fn ffsuffix(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &str,
	id: &Id,
	ft: &str,
	ff: &str,
) -> Result<Vec<u8>> {
	let mut k = PrefixFf::new(ns, db, tb, id, ft, ff).encode_key()?;
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
		id: Id,
		ft: &'a str,
		ff: &'a str,
		fk: Id,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'&',
			id,
			ft,
			ff,
			fk,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Ref::new(
			"testns",
			"testdb",
			"testtb",
			"testid".into(),
			"othertb",
			"test.*",
			"otherid".into(),
		);
		let enc = Ref::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\x00&\0\0\0\x01testid\0othertb\0test.*\0\0\0\0\x01otherid\0"
		);
	}
}
