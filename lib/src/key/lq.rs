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
	#[serde(with = "uuid::serde::compact")]
	pub lq: Uuid,
}

pub fn new<'a>(nd: Uuid, ns: &'a str, db: &'a str, lq: Uuid) -> Lq<'a> {
	Lq::new(nd, ns, db, lq)
}

impl<'a> Lq<'a> {
	pub fn new(nd: Uuid, ns: &'a str, db: &'a str, lq: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'n',
			_c: b'd',
			nd,
			_d: b'*',
			ns,
			_e: b'*',
			db,
			_f: b'!',
			_g: b'l',
			_h: b'v',
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
