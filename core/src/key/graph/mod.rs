//! Stores a graph edge pointer
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::sql::dir::Dir;
use crate::sql::id::Id;
use crate::sql::thing::Thing;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
struct Prefix<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: Id,
}

impl<'a> Prefix<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, id: &Id) -> Self {
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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
struct PrefixEg<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: Id,
	pub eg: Dir,
}

impl<'a> PrefixEg<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, id: &Id, eg: &Dir) -> Self {
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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
struct PrefixFt<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: Id,
	pub eg: Dir,
	pub ft: &'a str,
}

impl<'a> PrefixFt<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, id: &Id, eg: &Dir, ft: &'a str) -> Self {
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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
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
	pub id: Id,
	pub eg: Dir,
	pub ft: &'a str,
	pub fk: Id,
}

pub fn new<'a>(
	ns: &'a str,
	db: &'a str,
	tb: &'a str,
	id: &Id,
	eg: &Dir,
	fk: &'a Thing,
) -> Graph<'a> {
	Graph::new(ns, db, tb, id.to_owned(), eg.to_owned(), fk)
}

pub fn prefix(ns: &str, db: &str, tb: &str, id: &Id) -> Vec<u8> {
	let mut k = Prefix::new(ns, db, tb, id).encode().unwrap();
	k.extend_from_slice(&[0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, tb: &str, id: &Id) -> Vec<u8> {
	let mut k = Prefix::new(ns, db, tb, id).encode().unwrap();
	k.extend_from_slice(&[0xff]);
	k
}

pub fn egprefix(ns: &str, db: &str, tb: &str, id: &Id, eg: &Dir) -> Vec<u8> {
	let mut k = PrefixEg::new(ns, db, tb, id, eg).encode().unwrap();
	k.extend_from_slice(&[0x00]);
	k
}

pub fn egsuffix(ns: &str, db: &str, tb: &str, id: &Id, eg: &Dir) -> Vec<u8> {
	let mut k = PrefixEg::new(ns, db, tb, id, eg).encode().unwrap();
	k.extend_from_slice(&[0xff]);
	k
}

pub fn ftprefix(ns: &str, db: &str, tb: &str, id: &Id, eg: &Dir, ft: &str) -> Vec<u8> {
	let mut k = PrefixFt::new(ns, db, tb, id, eg, ft).encode().unwrap();
	k.extend_from_slice(&[0x00]);
	k
}

pub fn ftsuffix(ns: &str, db: &str, tb: &str, id: &Id, eg: &Dir, ft: &str) -> Vec<u8> {
	let mut k = PrefixFt::new(ns, db, tb, id, eg, ft).encode().unwrap();
	k.extend_from_slice(&[0xff]);
	k
}

impl Categorise for Graph<'_> {
	fn categorise(&self) -> Category {
		Category::Graph
	}
}

impl<'a> Graph<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, id: Id, eg: Dir, fk: &'a Thing) -> Self {
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
			ft: &fk.tb,
			fk: fk.id.to_owned(),
		}
	}

	pub fn new_from_id(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		id: Id,
		eg: Dir,
		ft: &'a str,
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
	#[test]
	fn key() {
		use super::*;
		use crate::syn::Parse;
		let fk = Thing::parse("other:test");
		#[rustfmt::skip]
		let val = Graph::new(
			"testns",
			"testdb",
			"testtb",
			"testid".into(),
			Dir::Out,
			&fk,
		);
		let enc = Graph::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\x00~\0\0\0\x01testid\0\0\0\0\x01other\0\0\0\0\x01test\0"
		);

		let dec = Graph::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
