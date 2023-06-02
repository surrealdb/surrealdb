use derive::Key;
use nom::AsBytes;
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

pub fn prefix_nd(nd: &Uuid) -> Vec<u8> {
	let mut k = [b'/', b'!', b'n', b'd'].to_vec();
	k.extend_from_slice(
		storekey::serialize(nd).expect("failed to serialise uuid for prefix").as_bytes(),
	);
	k
}

pub fn suffix_nd(nd: &Uuid) -> Vec<u8> {
	let mut k = [b'/', b'!', b'n', b'd'].to_vec();
	k.extend_from_slice(
		storekey::serialize(Uuid::from_u128(nd.as_u128() + 1).as_ref())
			.expect("failed to serialise uuid for suffix")
			.as_bytes(),
	);
	k
}

pub fn _prefix_db(nd: &Uuid, ns: &str, db: &str) -> Vec<u8> {
	let mut k = [b'/', b'!', b'n', b'd'].to_vec();
	k.extend_from_slice(nd.as_bytes());
	k.extend_from_slice(b"*");
	k.extend_from_slice(ns.as_bytes());
	k.extend_from_slice(b"*");
	k.extend_from_slice(db.as_bytes());
	k
}

pub fn _suffix_db(nd: &Uuid, ns: &str, db: &str) -> Vec<u8> {
	let mut k = [b'/', b'!', b'n', b'd'].to_vec();
	k.extend_from_slice(nd.as_bytes());
	k.extend_from_slice(b"*");
	k.extend_from_slice(ns.as_bytes());
	k.extend_from_slice(b"*");
	// TODO use BigInt to add 1 to the db name
	k.extend_from_slice(db.as_bytes());
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

	#[test]
	fn prefix_nd() {
		use super::*;
		let nd = Uuid::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
		let val = prefix_nd(&nd);
		assert_eq!(val, b"/!nd\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10");
	}

	#[test]
	fn suffix_nd() {
		use super::*;
		let nd = Uuid::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
		let val = suffix_nd(&nd);
		assert_eq!(val, b"/!nd\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x11");
	}
}
