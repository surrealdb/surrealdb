//! Stores Things of an HNSW index
use crate::idx::trees::vector::SerializedVector;
use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
use derive::Key;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Hv<'a> {
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
	pub vec: Arc<SerializedVector>,
}

impl<'a> KeyRequirements for Hv<'a> {
	fn key_category(&self) -> KeyCategory {
		KeyCategory::IndexHnswVec
	}
}
impl<'a> Hv<'a> {
	pub fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		vec: Arc<SerializedVector>,
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
			_f: b'h',
			_g: b'v',
			vec,
		}
	}
}

#[cfg(test)]
mod tests {

	#[test]
	fn key() {
		use super::*;
		let val = Hv::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			Arc::new(SerializedVector::I16(vec![5])),
		);
		let enc = Hv::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0+testix\0!hv\x3F\xF0\0\0\0\0\0\0\x40\0\0\0\0\0\0\0\x40\x08\0\0\0\0\0\0\x01",
			"{}",
			String::from_utf8_lossy(&enc)
		);

		let dec = Hv::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
