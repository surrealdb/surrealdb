//! Stores MTree state and nodes
use crate::idx::trees::store::NodeId;
use derive::Key;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Vm<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	_f: u8,
	_g: u8,
	pub node_id: Option<NodeId>,
}

impl<'a> Vm<'a> {
	pub fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		node_id: Option<NodeId>,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'+',
			ix,
			_e: b'!',
			_f: b'v',
			_g: b'm',
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
		let val = Vm::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			Some(8)
		);
		let enc = Vm::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!vm\x01\0\0\0\0\0\0\0\x08");

		let dec = Vm::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
