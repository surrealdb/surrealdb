use crate::sql::uuid::Uuid;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Lq<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	_d: u8,
	_e: u8,
	pub lq: Uuid,
}

pub fn new<'a>(ns: &'a str, db: &'a str, lq: &Uuid) -> Lq<'a> {
	Lq::new(ns, db, lq.to_owned())
}

impl<'a> Lq<'a> {
	pub fn new(ns: &'a str, db: &'a str, lq: Uuid) -> Self {
		Self {
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
			"test",
			"test",
			Uuid::default(),
		);
		let enc = Lq::encode(&val).unwrap();
		let dec = Lq::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
