//! Stores a graph edge pointer
use crate::sql::dir::Dir;
use crate::sql::id::Id;
use crate::sql::thing::Thing;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
struct Prefix {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	pub tb: u32,
	_d: u8,
	pub id: Id,
}

impl Prefix {
	fn new(ns: u32, db: u32, tb: u32, id: &Id) -> Self {
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
struct PrefixEg {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	pub tb: u32,
	_d: u8,
	pub id: Id,
	pub eg: Dir,
}

impl PrefixEg {
	fn new(ns: u32, db: u32, tb: u32, id: &Id, eg: &Dir) -> Self {
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
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	pub tb: u32,
	_d: u8,
	pub id: Id,
	pub eg: Dir,
	pub ft: &'a str,
}

impl<'a> PrefixFt<'a> {
	fn new(ns: u32, db: u32, tb: u32, id: &Id, eg: &Dir, ft: &'a str) -> Self {
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
pub struct Graph<'a> {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_c: u8,
	pub tb: u32,
	_d: u8,
	pub id: Id,
	pub eg: Dir,
	pub ft: &'a str,
	pub fk: Id,
}

pub fn new<'a>(ns: u32, db: u32, tb: u32, id: &Id, eg: &Dir, fk: &'a Thing) -> Graph<'a> {
	Graph::new(ns, db, tb, id.to_owned(), eg.to_owned(), fk)
}

pub fn prefix(ns: u32, db: u32, tb: u32, id: &Id) -> Vec<u8> {
	let mut k = Prefix::new(ns, db, tb, id).encode().unwrap();
	k.extend_from_slice(&[0x00]);
	k
}

pub fn suffix(ns: u32, db: u32, tb: u32, id: &Id) -> Vec<u8> {
	let mut k = Prefix::new(ns, db, tb, id).encode().unwrap();
	k.extend_from_slice(&[0xff]);
	k
}

pub fn egprefix(ns: u32, db: u32, tb: u32, id: &Id, eg: &Dir) -> Vec<u8> {
	let mut k = PrefixEg::new(ns, db, tb, id, eg).encode().unwrap();
	k.extend_from_slice(&[0x00]);
	k
}

pub fn egsuffix(ns: u32, db: u32, tb: u32, id: &Id, eg: &Dir) -> Vec<u8> {
	let mut k = PrefixEg::new(ns, db, tb, id, eg).encode().unwrap();
	k.extend_from_slice(&[0xff]);
	k
}

pub fn ftprefix(ns: u32, db: u32, tb: u32, id: &Id, eg: &Dir, ft: &str) -> Vec<u8> {
	let mut k = PrefixFt::new(ns, db, tb, id, eg, ft).encode().unwrap();
	k.extend_from_slice(&[0x00]);
	k
}

pub fn ftsuffix(ns: u32, db: u32, tb: u32, id: &Id, eg: &Dir, ft: &str) -> Vec<u8> {
	let mut k = PrefixFt::new(ns, db, tb, id, eg, ft).encode().unwrap();
	k.extend_from_slice(&[0xff]);
	k
}

impl<'a> Graph<'a> {
	pub fn new(ns: u32, db: u32, tb: u32, id: Id, eg: Dir, fk: &'a Thing) -> Self {
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
			1,
			2,
			3,
			"testid".into(),
			Dir::Out,
			&fk,
		);
		let enc = Graph::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*\x00\x00\x00\x03~\0\0\0\x01testid\0\0\0\0\x01other\0\0\0\0\x01test\0"
		);

		let dec = Graph::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
