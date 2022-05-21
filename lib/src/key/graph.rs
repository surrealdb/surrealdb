use crate::err::Error;
use crate::sql::graph::Dir;
use crate::sql::id::Id;
use crate::sql::thing::Thing;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
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
	pub fk: Thing,
}

impl From<Graph> for Vec<u8> {
	fn from(val: Graph) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Graph {
	fn from(val: Vec<u8>) -> Self {
		Graph::decode(&val).unwrap()
	}
}

impl From<&Vec<u8>> for Graph {
	fn from(val: &Vec<u8>) -> Self {
		Graph::decode(val).unwrap()
	}
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
	let mut k = super::thing::new(ns, db, tb, id).encode().unwrap();
	k.extend_from_slice(&[0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, tb: &str, id: &Id) -> Vec<u8> {
	let mut k = super::thing::new(ns, db, tb, id).encode().unwrap();
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
			fk,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		crate::sql::serde::beg_internal_serialization();
		let v = serialize(self);
		crate::sql::serde::end_internal_serialization();
		Ok(v?)
	}
	pub fn decode(v: &[u8]) -> Result<Graph, Error> {
		Ok(deserialize(v)?)
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
