use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use crate::sql::value::Value;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Index {
	kv: String,
	_a: String,
	ns: String,
	_b: String,
	db: String,
	_c: String,
	tb: String,
	_d: String,
	ix: String,
	fd: Value,
}

impl Into<Vec<u8>> for Index {
	fn into(self) -> Vec<u8> {
		self.encode().unwrap()
	}
}

impl From<Vec<u8>> for Index {
	fn from(val: Vec<u8>) -> Self {
		Index::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, tb: &str, ix: &str, fd: Value) -> Index {
	Index::new(ns.to_string(), db.to_string(), tb.to_string(), ix.to_string(), fd)
}

impl Index {
	pub fn new(ns: String, db: String, tb: String, ix: String, fd: Value) -> Index {
		Index {
			kv: BASE.to_owned(),
			_a: String::from("*"),
			ns,
			_b: String::from("*"),
			db,
			_c: String::from("*"),
			tb,
			_d: String::from("¤"),
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
