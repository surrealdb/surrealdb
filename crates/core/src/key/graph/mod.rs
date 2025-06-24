//! Stores a graph edge pointer
use crate::expr::dir::Dir;
use crate::key::category::{Categorise, Category};
use crate::kvs::{KeyEncode, impl_key};
use crate::val::{RecordId, RecordIdKey};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
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
impl_key!(Prefix<'a>);

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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
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
impl_key!(PrefixEg<'a>);

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
			id: id.to_owned(),
			eg: eg.to_owned(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
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
impl_key!(PrefixFt<'a>);

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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Graph<'a> {
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
impl_key!(Graph<'a>);

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
	let mut k = Prefix::new(ns, db, tb, id).encode()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

pub fn suffix(ns: &str, db: &str, tb: &str, id: &RecordIdKey) -> Result<Vec<u8>> {
	let mut k = Prefix::new(ns, db, tb, id).encode()?;
	k.extend_from_slice(&[0xff]);
	Ok(k)
}

pub fn egprefix(ns: &str, db: &str, tb: &str, id: &RecordIdKey, eg: &Dir) -> Result<Vec<u8>> {
	let mut k = PrefixEg::new(ns, db, tb, id, eg).encode()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

pub fn egsuffix(ns: &str, db: &str, tb: &str, id: &RecordIdKey, eg: &Dir) -> Result<Vec<u8>> {
	let mut k = PrefixEg::new(ns, db, tb, id, eg).encode()?;
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
	let mut k = PrefixFt::new(ns, db, tb, id, eg, ft).encode()?;
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
	let mut k = PrefixFt::new(ns, db, tb, id, eg, ft).encode()?;
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

// #[cfg(test)]
// mod tests {
// 	use super::*;
// 	use crate::kvs::KeyDecode;
//
// 	#[test]
// 	fn key() {
// 		use crate::syn::Parse;
//
// 		let fk: RecordId = RecordId::parse("other:test").into();
// 		#[rustfmt::skip]
// 		let val = Graph::new(
// 			"testns",
// 			"testdb",
// 			"testtb",
// 			"testid".into(),
// 			Dir::Out,
// 			&fk,
// 		);
// 		let enc = Graph::encode(&val).unwrap();
// 		assert_eq!(
// 			enc,
// 			b"/*testns\0*testdb\0*testtb\x00~\0\0\0\x01testid\0\0\0\0\x01other\0\0\0\0\x01test\0"
// 		);
//
// 		let dec = Graph::decode(&enc).unwrap();
// 		assert_eq!(val, dec);
// 	}
// }
