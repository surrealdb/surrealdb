//! Store appended records for concurrent index building
use crate::kvs::{impl_key, KeyEncode};
use crate::{err::Error, key::index::all};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Ib<'a> {
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
	pub appending_id: u32,
	pub batch_id: u32,
}
impl_key!(Ib<'a>);

impl<'a> Ib<'a> {
	#[cfg_attr(target_family = "wasm", allow(dead_code))]
	pub fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		appending_id: u32,
		batch_id: u32,
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
			_f: b'i',
			_g: b'b',
			appending_id,
			batch_id,
		}
	}
}

/// Returns the prefix for all index appending keys for the given index
fn prefix(ns: &str, db: &str, tb: &str, ix: &str) -> Result<Vec<u8>, Error> {
	let mut k = all::new(ns, db, tb, ix).encode()?;
	k.extend_from_slice(b"!ib");
	Ok(k)
}

/// Returns the key range that bounds all index appending keys for the given index.
pub(crate) fn range(ns: &str, db: &str, tb: &str, ix: &str) -> Result<(Vec<u8>, Vec<u8>), Error> {
	let mut end = prefix(ns, db, tb, ix)?;
	let mut beg = end.clone();
	beg.extend_from_slice(&[0x00]);
	end.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
	Ok((beg, end))
}

#[cfg(test)]
mod tests {
	use crate::kvs::KeyDecode;

	#[test]
	fn key() {
		use super::*;
		let val = Ib::new("testns", "testdb", "testtb", "testix", 1, 2);
		let enc = Ib::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0+testix\0!ib\x00\x00\x00\x01\x00\x00\x00\x02",
			"{}",
			String::from_utf8_lossy(&enc)
		);

		let dec = Ib::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
