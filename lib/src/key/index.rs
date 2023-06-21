use crate::key::CHAR_INDEX;
use crate::sql::array::Array;
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
	pub ix: &'a str,
	_e: u8,
}

impl<'a> Prefix<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: CHAR_INDEX,
			ix,
			_e: b'*',
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
struct PrefixIds<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	pub fd: Array,
	_f: u8,
}

impl<'a> PrefixIds<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, fd: &Array) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: CHAR_INDEX,
			ix,
			_e: b'*',
			fd: fd.to_owned(),
			_f: b'*',
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Index<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	pub fd: Array,
	_f: u8,
	pub id: Option<Id>,
}

pub fn new<'a>(
	ns: &'a str,
	db: &'a str,
	tb: &'a str,
	ix: &'a str,
	fd: &Array,
	id: Option<&Id>,
) -> Index<'a> {
	Index::new(ns, db, tb, ix, fd.to_owned(), id.cloned())
}

pub fn prefix(ns: &str, db: &str, tb: &str, ix: &str) -> Vec<u8> {
	let mut k = Prefix::new(ns, db, tb, ix).encode().unwrap();
	k.extend_from_slice(&[0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, tb: &str, ix: &str) -> Vec<u8> {
	let mut k = Prefix::new(ns, db, tb, ix).encode().unwrap();
	k.extend_from_slice(&[0xff]);
	k
}

pub fn prefix_all_ids(ns: &str, db: &str, tb: &str, ix: &str, fd: &Array) -> Vec<u8> {
	let mut k = PrefixIds::new(ns, db, tb, ix, fd).encode().unwrap();
	k.extend_from_slice(&[0x00]);
	k
}

pub fn suffix_all_ids(ns: &str, db: &str, tb: &str, ix: &str, fd: &Array) -> Vec<u8> {
	let mut k = PrefixIds::new(ns, db, tb, ix, fd).encode().unwrap();
	k.extend_from_slice(&[0xff]);
	k
}

impl<'a> Index<'a> {
	pub fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		fd: Array,
		id: Option<Id>,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: CHAR_INDEX,
			ix,
			_e: 0x2a, // *
			fd,
			_f: 0x2a, // *
			id,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Index::new(
			"test",
			"test",
			"test",
			"test",
			vec!["test"].into(),
			Some("test".into()),
		);
		let enc = Index::encode(&val).unwrap();
		let dec = Index::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
