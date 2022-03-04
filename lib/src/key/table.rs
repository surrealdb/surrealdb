use crate::err::Error;
use crate::key::bytes::{deserialize, serialize};
use crate::key::BASE;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Table {
	kv: String,
	_a: String,
	ns: String,
	_b: String,
	db: String,
	_c: String,
	tb: String,
}

impl From<Table> for Vec<u8> {
	fn from(val: Table) -> Vec<u8> {
		val.encode().unwrap()
	}
}

impl From<Vec<u8>> for Table {
	fn from(val: Vec<u8>) -> Self {
		Table::decode(&val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, tb: &str) -> Table {
	Table::new(ns.to_string(), db.to_string(), tb.to_string())
}

impl Table {
	pub fn new(ns: String, db: String, tb: String) -> Table {
		Table {
			kv: BASE.to_owned(),
			_a: String::from("*"),
			ns,
			_b: String::from("*"),
			db,
			_c: String::from("*"),
			tb,
		}
	}
	pub fn encode(&self) -> Result<Vec<u8>, Error> {
		Ok(serialize(self)?)
	}
	pub fn decode(v: &[u8]) -> Result<Table, Error> {
		Ok(deserialize(v)?)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Table::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
		);
		let enc = Table::encode(&val).unwrap();
		let dec = Table::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
