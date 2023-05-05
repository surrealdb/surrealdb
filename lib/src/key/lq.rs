use derive::Key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Lq<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub nd: Uuid,
	_d: u8,
	pub ns: &'a str,
	_e: u8,
	pub db: &'a str,
	_f: u8,
	_g: u8,
	_h: u8,
	pub lq: Uuid,
}

pub fn new<'a>(nd: &Uuid, ns: &'a str, db: &'a str, lq: &Uuid) -> Lq<'a> {
	Lq::new(nd.to_owned(), ns, db, lq.to_owned())
}

impl<'a> Lq<'a> {
	pub fn new(nd: Uuid, ns: &'a str, db: &'a str, lq: Uuid) -> Self {
		Self {
			__: 0x2f, // /
			_a: 0x21, // !
			_b: 0x6e, // n
			_c: 0x64, // d
			nd,
			_d: 0x2a, // *
			ns,
			_e: 0x2a, // *
			db,
			_f: 0x21, // !
			_g: 0x6c, // l
			_h: 0x71, // v
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
			Uuid::default(),
			"test",
			"test",
			Uuid::default(),
		);
		let enc = Lq::encode(&val).unwrap();
		let dec = Lq::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
