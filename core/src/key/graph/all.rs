//! Stores the key prefix for all keys under a table
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::sql::Id;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct GraphRoot<'a> {
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

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str, id: Id) -> GraphRoot<'a> {
	GraphRoot::new(ns, db, tb, id)
}

impl Categorise for GraphRoot<'_> {
	fn categorise(&self) -> Category {
		Category::GraphRoot
	}
}

impl<'a> GraphRoot<'a> {
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
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = GraphRoot::new(
			"testns",
			"testdb",
			"testtb",
			"testid".into(),
		);
		let enc = GraphRoot::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0~testid\0");

		let dec = GraphRoot::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
