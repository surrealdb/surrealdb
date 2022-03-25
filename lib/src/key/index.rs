use crate::err::Error;
use crate::sql::value::Value;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
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
	pub fd: Value,
}

impl From<Index> for Vec<u8> {
	fn from(val: Index) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Index {
	fn from(val: Vec<u8>) -> Self {
		Index::decode(&val).unwrap()
	}
}

impl From<&Vec<u8>> for Index {
	fn from(val: &Vec<u8>) -> Self {
		Index::decode(val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, tb: &str, ix: &str, fd: Value) -> Index {
	Index::new(ns.to_string(), db.to_string(), tb.to_string(), ix.to_string(), fd)
}

pub fn prefix(ns: &str, db: &str, tb: &str, ix: &str) -> Vec<u8> {
	let mut k = super::guide::new(ns, db, tb, ix).encode().unwrap();
	k.extend_from_slice(&[0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, tb: &str, ix: &str) -> Vec<u8> {
	let mut k = super::guide::new(ns, db, tb, ix).encode().unwrap();
	k.extend_from_slice(&[0xff]);
	k
}

impl Index {
	pub fn new(ns: String, db: String, tb: String, ix: String, fd: Value) -> Index {
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
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Index, Error> {
		Ok(deserialize(v)?)
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
			"test".into(),
		);
		let enc = Index::encode(&val).unwrap();
		let dec = Index::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
