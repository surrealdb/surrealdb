/// Stores a DEFINE NAMESPACE config definition
use derive::Key;
use serde::{Deserialize, Serialize};

pub mod az;
pub mod bc;
pub mod bd;
pub mod bf;
pub mod bi;
pub mod bk;
pub mod bl;
pub mod bo;
pub mod bp;
pub mod bs;
pub mod bt;
pub mod bu;
pub mod database;
pub mod db;
pub mod dl;
pub mod dt;
pub mod dv;
pub mod ev;
pub mod fc;
pub mod fd;
pub mod ft;
pub mod graph;
pub mod index;
pub mod ix;
pub mod lv;
pub mod namespace;
pub mod nl;
pub mod nt;
pub mod pa;
pub mod sc;
pub mod scope;
pub mod st;
pub mod table;
pub mod tb;
pub mod thing;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Ns<'a> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub ns: &'a str,
}

pub fn new(ns: &str) -> Ns<'_> {
	Ns::new(ns)
}

pub fn prefix() -> Vec<u8> {
	let mut k = super::kv::new().encode().unwrap();
	k.extend_from_slice(&[b'!', b'n', b's', 0x00]);
	k
}

pub fn suffix() -> Vec<u8> {
	let mut k = super::kv::new().encode().unwrap();
	k.extend_from_slice(&[b'!', b'n', b's', 0xff]);
	k
}

impl<'a> Ns<'a> {
	pub fn new(ns: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'n',
			_c: b's',
			ns,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
            let val = Ns::new(
            "testns",
        );
		let enc = Ns::encode(&val).unwrap();
		assert_eq!(enc, b"/!nstestns\0");

		let dec = Ns::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
