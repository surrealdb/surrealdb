//! Stores a graph edge pointer
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::sql::id::Id;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct GraphCount<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: Id,
	_e: u8,
	_f: u8,
	_g: u8,
}

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str, id: &Id) -> GraphCount<'a> {
	GraphCount::new(ns, db, tb, id.to_owned())
}

impl Categorise for GraphCount<'_> {
	fn categorise(&self) -> Category {
		Category::GraphCount
	}
}

impl<'a> GraphCount<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, id: Id) -> Self {
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
			_e: b'!',
			_f: b'g',
			_g: b'c',
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = GraphCount::new(
			"testns",
			"testdb",
			"testtb",
			"testid".into(),
		);
		let enc = GraphCount::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\x00~\0\0\0\x01testid\0!gc");

		let dec = GraphCount::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
