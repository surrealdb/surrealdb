//! Stores a LIVE SELECT query definition on the cluster
use derive::Key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Lq<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	#[serde(with = "uuid::serde::compact")]
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

pub fn prefix_nd(nd: &Uuid) -> Vec<u8> {
	let mut k = [b'/', b'!', b'n', b'd'].to_vec();
	k.extend_from_slice(nd.as_bytes());
	k.extend_from_slice(&[0x00]);
	k
}

pub fn suffix_nd(nd: &Uuid) -> Vec<u8> {
	let mut k = [b'/', b'!', b'n', b'd'].to_vec();
	k.extend_from_slice(nd.as_bytes());
	k.extend_from_slice(&[0xff]);
	k
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
			_g: b'n',
			_h: b'q',
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
		let nd = Uuid::from_bytes([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10]);
		#[rustfmt::skip]
		let lv = Uuid::from_bytes([0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20]);
		let val = Lq::new(nd, "testns", "testdb", lv);
		let enc = Lq::encode(&val).unwrap();
		assert_eq!(enc, b"/!nd\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10*testns\x00*testdb\x00!nq\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x20" );

		let dec = Lq::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn prefix_nd() {
		use super::*;
		let nd = Uuid::from_bytes([
			0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
			0x0f, 0x10,
		]);
		let val = prefix_nd(&nd);
		assert_eq!(
			val,
			b"/!nd\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x00"
		);
	}

	#[test]
	fn suffix_nd() {
		use super::*;
		let nd = Uuid::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
		let val = suffix_nd(&nd);
		assert_eq!(
			val,
			b"/!nd\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\xff"
		);
	}
}
