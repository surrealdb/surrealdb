use crate::sql::array::Array;
use crate::sql::id::Id;
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
	pub ix: String,
}

impl Prefix {
	fn new(ns: &str, db: &str, tb: &str, ix: &str) -> Prefix {
		Prefix {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns: ns.to_string(),
			_b: 0x2a, // *
			db: db.to_string(),
			_c: 0x2a, // *
			tb: tb.to_string(),
			_d: 0xa4, // ¤
			ix: ix.to_string(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Index {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	pub tb: String,
	_d: u8,
	pub ix: String,
	pub fd: Array,
	pub id: Option<Id>,
}

pub fn new(ns: &str, db: &str, tb: &str, ix: &str, fd: Array, id: Option<&Id>) -> Index {
	Index::new(ns.to_string(), db.to_string(), tb.to_string(), ix.to_string(), fd, id.cloned())
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

impl Index {
	pub fn new(ns: String, db: String, tb: String, ix: String, fd: Array, id: Option<Id>) -> Index {
		Index {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0xa4, // ¤
			ix,
			fd,
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
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			vec!["test"].into(),
			Some("test".into()),
		);
		let enc = Index::encode(&val).unwrap();
		let dec = Index::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
