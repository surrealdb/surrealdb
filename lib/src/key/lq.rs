use crate::sql::uuid::Uuid;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Lq {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	_d: u8,
	_e: u8,
	pub lq: Uuid,
}

pub fn new(ns: &str, db: &str, lq: &Uuid) -> Lq {
	Lq::new(ns.to_string(), db.to_string(), lq.to_owned())
}

impl Lq {
	pub fn new(ns: String, db: String, lq: Uuid) -> Lq {
		Lq {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x21, // !
			_d: 0x6c, // l
			_e: 0x71, // v
			lq,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Lq::new(
			"test".to_string(),
			"test".to_string(),
			"00000000-0000-0000-0000-000000000000".into(),
		);
		let enc = Lq::encode(&val).unwrap();
		let dec = Lq::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
