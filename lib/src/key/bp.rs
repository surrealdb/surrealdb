use crate::idx::btree::NodeId;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Bp<'a> {
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
	pub node_id: Option<NodeId>,
}

impl<'a> Bp<'a> {
	pub fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		node_id: Option<NodeId>,
	) -> Self {
		Self {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0x21, // !
			_e: 0x62, // b
			_f: 0x70, // p
			ix,
			_g: 0x2a, // *
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
		let val = Bp::new(
			"test",
			"test",
			"test",
			"test",
			Some(7)
		);
		let enc = Bp::encode(&val).unwrap();
		let dec = Bp::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
