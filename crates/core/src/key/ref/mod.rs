//! Stores a graph edge pointer
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::sql::id::Id;
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
struct PrefixFf<'a> {
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
	pub ff: &'a str,
}

impl<'a> PrefixFf<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, id: &Id, ft: &'a str, ff: &'a str) -> Self {
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
	pub ff: &'a str,
	pub fk: Id,
}

pub fn new<'a>(
	ns: &'a str,
	db: &'a str,
	tb: &'a str,
	id: &Id,
	ft: &'a str,
	ff: &'a str,
	fk: &Id,
) -> Ref<'a> {
	Ref::new(ns, db, tb, id.to_owned(), ft, ff, fk.to_owned())
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

pub fn ffprefix(ns: &str, db: &str, tb: &str, id: &Id, ft: &str, ff: &str) -> Vec<u8> {
	let mut k = PrefixFf::new(ns, db, tb, id, ft, ff).encode().unwrap();
	k.extend_from_slice(&[0x00]);
	k
}

pub fn ffsuffix(ns: &str, db: &str, tb: &str, id: &Id, ft: &str, ff: &str) -> Vec<u8> {
	let mut k = PrefixFf::new(ns, db, tb, id, ft, ff).encode().unwrap();
	k.extend_from_slice(&[0xff]);
	k
}

impl Categorise for Ref<'_> {
	fn categorise(&self) -> Category {
		Category::Ref
	}
}

impl<'a> Ref<'a> {
	pub fn new(
		ns: &'a str,
		db: &'a str,
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
	#[test]
	fn key() {
		use super::*;
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
		let enc = Ref::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\x00&\0\0\0\x01testid\0othertb\0test.*\0\0\0\0\x01otherid\0"
		);

		let dec = Ref::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
