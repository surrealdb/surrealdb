use crate::err::Error;
use serde::{Deserialize, Serialize};
use storekey::{deserialize, serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Table {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	pub tb: String,
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

impl From<&Vec<u8>> for Table {
	fn from(val: &Vec<u8>) -> Self {
		Table::decode(val).unwrap()
	}
}

pub fn new(ns: &str, db: &str, tb: &str) -> Table {
	Table::new(ns.to_string(), db.to_string(), tb.to_string())
}

pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[0x2a, 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[0x2a, 0xff]);
	k
}

impl Table {
	pub fn new(ns: String, db: String, tb: String) -> Table {
		Table {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
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
