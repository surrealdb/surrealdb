use crate::idx::btree::NodeId;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Bl {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_c: u8,
	pub tb: String,
	_d: u8,
	_e: u8,
	_f: u8,
	pub ix: String,
	_g: u8,
	pub node_id: Option<NodeId>,
}

impl Bl {
	pub fn new(ns: String, db: String, tb: String, ix: String, node_id: Option<NodeId>) -> Bl {
		Bl {
			__: 0x2f, // /
			_a: 0x2a, // *
			ns,
			_b: 0x2a, // *
			db,
			_c: 0x2a, // *
			tb,
			_d: 0x21, // !
			_e: 0x62, // b
			_f: 0x6c, // l
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
		let val = Bl::new(
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			"test".to_string(),
			Some(7)
		);
		let enc = Bl::encode(&val).unwrap();
		let dec = Bl::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
