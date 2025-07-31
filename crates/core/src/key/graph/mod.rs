//! Stores a graph edge pointer
use crate::expr::dir::Dir;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use crate::val::{RecordId, RecordIdKey};

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Prefix<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: RecordIdKey,
}

impl KVKey for Prefix<'_> {
	type ValueType = Vec<u8>;
}

impl<'a> Prefix<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, id: &RecordIdKey) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'~',
			id: id.to_owned(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct PrefixEg<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: RecordIdKey,
	pub eg: Dir,
}

impl KVKey for PrefixEg<'_> {
	type ValueType = Vec<u8>;
}

impl<'a> PrefixEg<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, id: &RecordIdKey, eg: &Dir) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'~',
			id: id.clone(),
			eg: eg.clone(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct PrefixFt<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: RecordIdKey,
	pub eg: Dir,
	pub ft: &'a str,
}

impl KVKey for PrefixFt<'_> {
	type ValueType = Vec<u8>;
}

impl<'a> PrefixFt<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, id: &RecordIdKey, eg: &Dir, ft: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'~',
			id: id.to_owned(),
			eg: eg.to_owned(),
			ft,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Graph<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: RecordIdKey,
	pub eg: Dir,
	pub ft: &'a str,
	pub fk: RecordIdKey,
}

impl KVKey for Graph<'_> {
	type ValueType = ();
}

impl Graph<'_> {
	pub fn decode_key(k: &[u8]) -> Result<Graph<'_>> {
		Ok(storekey::deserialize(k)?)
	}
}

pub fn new<'a>(
	ns: &'a str,
	db: &'a str,
	tb: &'a str,
	id: &RecordIdKey,
	eg: &Dir,
	fk: &'a RecordId,
) -> Graph<'a> {
	Graph::new(ns, db, tb, id.to_owned(), eg.to_owned(), fk)
}

pub fn prefix(ns: &str, db: &str, tb: &str, id: &RecordIdKey) -> Result<Vec<u8>> {
	let mut k = Prefix::new(ns, db, tb, id).encode_key()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

pub fn suffix(ns: &str, db: &str, tb: &str, id: &RecordIdKey) -> Result<Vec<u8>> {
	let mut k = Prefix::new(ns, db, tb, id).encode_key()?;
	k.extend_from_slice(&[0xff]);
	Ok(k)
}

pub fn egprefix(ns: &str, db: &str, tb: &str, id: &RecordIdKey, eg: &Dir) -> Result<Vec<u8>> {
	let mut k = PrefixEg::new(ns, db, tb, id, eg).encode_key()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

pub fn egsuffix(ns: &str, db: &str, tb: &str, id: &RecordIdKey, eg: &Dir) -> Result<Vec<u8>> {
	let mut k = PrefixEg::new(ns, db, tb, id, eg).encode_key()?;
	k.extend_from_slice(&[0xff]);
	Ok(k)
}

pub fn ftprefix(
	ns: &str,
	db: &str,
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
	ns: &str,
	db: &str,
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
		ns: &'a str,
		db: &'a str,
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
			tb,
			_d: b'~',
			id,
			eg,
			ft: &fk.table,
			fk: fk.key.clone(),
		}
	}

	#[allow(dead_code)]
	pub fn new_from_id(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		id: RecordIdKey,
		eg: Dir,
		ft: &'a str,
		fk: RecordIdKey,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'~',
			id,
			eg,
			ft,
			fk,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use crate::syn;
	use crate::val::Value;

	#[test]
	fn key() {
		let Ok(Value::Thing(fk)) = syn::value("other:test") else {
			panic!()
		};
		#[rustfmt::skip]
		let val = Graph::new(
			"testns",
			"testdb",
			"testtb",
			strand!("testid").to_owned().into(),
			Dir::Out,
			&fk,
		);
		let enc = Graph::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\x00~\0\0\0\x01testid\0\0\0\0\x01other\0\0\0\0\x01test\0"
		);
	}
}
