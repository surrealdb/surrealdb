use crate::sql::dir::Dir;
use crate::sql::id::Id;
use crate::sql::thing::Thing;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
struct Prefix {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	pub tb: String,
	_d: u8,
	pub id: Id,
}

impl Prefix {
	fn new(ns: &str, db: &str, tb: &str, id: &Id) -> Prefix {
		Prefix {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns: ns.to_string(),
			_b: 0x2a, // *
			db: db.to_string(),
			_c: 0x2a, // *
			tb: tb.to_string(),
			_d: 0x7e, // ~
			id: id.to_owned(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
struct PrefixEg {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	pub tb: String,
	_d: u8,
	pub id: Id,
	pub eg: Dir,
}

impl PrefixEg {
	fn new(ns: &str, db: &str, tb: &str, id: &Id, eg: &Dir) -> PrefixEg {
		PrefixEg {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns: ns.to_string(),
			_b: 0x2a, // *
			db: db.to_string(),
			_c: 0x2a, // *
			tb: tb.to_string(),
			_d: 0x7e, // ~
			id: id.to_owned(),
			eg: eg.to_owned(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
struct PrefixFt {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	pub tb: String,
	_d: u8,
	pub id: Id,
	pub eg: Dir,
	pub ft: String,
}

impl PrefixFt {
	fn new(ns: &str, db: &str, tb: &str, id: &Id, eg: &Dir, ft: &str) -> PrefixFt {
		PrefixFt {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns: ns.to_string(),
			_b: 0x2a, // *
			db: db.to_string(),
			_c: 0x2a, // *
			tb: tb.to_string(),
			_d: 0x7e, // ~
			id: id.to_owned(),
			eg: eg.to_owned(),
			ft: ft.to_string(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Graph {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	pub tb: String,
	_d: u8,
	pub id: Id,
	pub eg: Dir,
	pub ft: String,
	pub fk: Id,
}

pub fn new(ns: &str, db: &str, tb: &str, id: &Id, eg: &Dir, fk: &Thing) -> Graph {
	Graph::new(
		ns.to_string(),
		db.to_string(),
		tb.to_string(),
		id.to_owned(),
		eg.to_owned(),
		fk.to_owned(),
	)
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

impl Graph {
	pub fn new(ns: String, db: String, tb: String, id: Id, eg: Dir, fk: Thing) -> Graph {
		Graph {
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
			ft: fk.tb,
			fk: fk.id,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		use crate::sql::test::Parse;
		#[rustfmt::skip]
		let val = Graph::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".into(),
			Dir::Out,
			Thing::parse("other:test"),
		);
		let enc = Graph::encode(&val).unwrap();
		let dec = Graph::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
