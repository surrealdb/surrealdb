//! Stores a graph edge pointer
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::sql::id::Id;
use crate::sql::thing::Thing;
use crate::sql::Idiom;
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
			_d: b'&',
			id: id.to_owned(),
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
	pub ft: &'a str,
}

impl<'a> PrefixFt<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, id: &Id, ft: &'a str) -> Self {
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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Ref<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: Id,
	pub ft: &'a str,
	pub fk: Id,
	pub ff: Idiom,
}

pub fn new<'a>(
	ns: &'a str,
	db: &'a str,
	tb: &'a str,
	id: &Id,
	fk: &'a Thing,
	ff: &'a Idiom,
) -> Ref<'a> {
	Ref::new(ns, db, tb, id.to_owned(), fk, ff)
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

pub fn ftprefix(ns: &str, db: &str, tb: &str, id: &Id, ft: &str) -> Vec<u8> {
	let mut k = PrefixFt::new(ns, db, tb, id, ft).encode().unwrap();
	k.extend_from_slice(&[0x00]);
	k
}

pub fn ftsuffix(ns: &str, db: &str, tb: &str, id: &Id, ft: &str) -> Vec<u8> {
	let mut k = PrefixFt::new(ns, db, tb, id, ft).encode().unwrap();
	k.extend_from_slice(&[0xff]);
	k
}

impl Categorise for Ref<'_> {
	fn categorise(&self) -> Category {
		Category::Ref
	}
}

impl<'a> Ref<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, id: Id, fk: &'a Thing, ff: &'a Idiom) -> Self {
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
			ft: &fk.tb,
			fk: fk.id.to_owned(),
			ff: ff.to_owned(),
		}
	}

	pub fn new_from_id(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		id: Id,
		ft: &'a str,
		fk: Id,
		ff: Idiom,
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
			fk,
			ff,
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
		let ff = Idiom::parse("test.*");
		#[rustfmt::skip]
		let val = Ref::new(
			"testns",
			"testdb",
			"testtb",
			"testid".into(),
			&fk,
			&ff,
		);
		let enc = Ref::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\x00&\0\0\0\x01testid\0other\0\0\0\0\x01test\0"
		);

		let dec = Ref::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
