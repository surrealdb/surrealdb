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
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0x7e, // ~
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
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0x7e, // ~
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
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0x7e, // ~
			id: id.to_owned(),
			eg: eg.to_owned(),
			ft,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
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

impl<'a> Graph<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, id: Id, eg: Dir, fk: &'a Thing) -> Self {
		Self {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0x7e, // ~
			id,
			eg,
			ft: &fk.tb,
			fk: fk.id.to_owned(),
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		use crate::sql::test::Parse;
		let fk = Thing::parse("other:test");
		#[rustfmt::skip]
		let val = Graph::new(
			"test",
			"test",
			"test",
			"test".into(),
			Dir::Out,
			&fk,
		);
		let enc = Graph::encode(&val).unwrap();
		let dec = Graph::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
