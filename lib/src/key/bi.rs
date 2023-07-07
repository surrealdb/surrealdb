use crate::idx::btree::NodeId;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Bi<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	_e: u8,
	_f: u8,
	pub ix: &'a str,
	_g: u8,
	pub node_id: NodeId,
}

impl<'a> Bi<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, node_id: NodeId) -> Self {
		Bi {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'!',
			_e: b'b',
			_f: b'i',
			ix,
			_g: b'*',
			node_id,
		}
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Bi::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			7
		);
		let enc = Bi::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0!bitestix\0*\
			\0\0\0\0\0\0\0\x07"
		);

		let dec = Bi::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
