use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Table {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	pub tb: String,
}

pub fn new(ns: &str, db: &str, tb: &str) -> Table {
	Table::new(ns.to_string(), db.to_string(), tb.to_string())
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
