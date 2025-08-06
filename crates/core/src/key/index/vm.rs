//! Stores MTree state and nodes
use crate::idx::trees::{mtree::MState, store::NodeId};
use crate::kvs::KVKey;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct VmRoot<'a> {
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
}

impl KVKey for VmRoot<'_> {
	type ValueType = MState;
}

impl<'a> VmRoot<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str) -> Self {
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
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Vm<'a> {
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
	pub node_id: NodeId,
}

impl KVKey for Vm<'_> {
	type ValueType = Vec<u8>;
}

impl<'a> Vm<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, node_id: NodeId) -> Self {
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
	use super::*;

	#[test]
	fn root() {
		let val = VmRoot::new("testns", "testdb", "testtb", "testix");
		let enc = VmRoot::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!vm");
	}

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Vm::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			8
		);
		let enc = Vm::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!vm\0\0\0\0\0\0\0\x08");
	}
}
