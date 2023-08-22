/// Stores change feeds
use derive::Key;
use serde::{Deserialize, Serialize};

use crate::vs;

use std::str;

// Cf stands for change feeds
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Cf {
	__: u8,
	_a: u8,
	pub ns: u32,
	_b: u8,
	pub db: u32,
	_d: u8,
	// vs is the versionstamp of the change feed entry that is encoded in big-endian.
	// Use the to_u64_be function to convert it to a u128.
	pub vs: [u8; 10],
	_c: u8,
	pub tb: u32,
}

#[allow(unused)]
pub fn new(ns: u32, db: u32, ts: u64, tb: u32) -> Cf {
	Cf::new(ns, db, vs::u64_to_versionstamp(ts), tb)
}

#[allow(unused)]
pub fn versionstamped_key_prefix(ns: u32, db: u32) -> Vec<u8> {
	let mut k = crate::key::database::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'#']);
	k
}

#[allow(unused)]
pub fn versionstamped_key_suffix(tb: u32) -> Vec<u8> {
	let mut k: Vec<u8> = vec![];
	k.extend_from_slice(&[b'*']);
	k.extend_from_slice(&tb.to_be_bytes());
	// Without this, decoding fails with Un&ectedEOF errors
	k.extend_from_slice(&[0x00]);
	k
}

/// Returns the prefix for the whole database change feeds since the
/// specified versionstamp.
#[allow(unused)]
pub fn prefix_ts(ns: u32, db: u32, vs: vs::Versionstamp) -> Vec<u8> {
	let mut k = crate::key::database::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'#']);
	k.extend_from_slice(&vs);
	k
}

/// Returns the prefix for the whole database change feeds
#[allow(unused)]
pub fn prefix(ns: u32, db: u32) -> Vec<u8> {
	let mut k = crate::key::database::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'#']);
	k
}

/// Returns the suffix for the whole database change feeds
#[allow(unused)]
pub fn suffix(ns: u32, db: u32) -> Vec<u8> {
	let mut k = crate::key::database::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'#', 0xff]);
	k
}

impl Cf {
	pub fn new(ns: u32, db: u32, vs: [u8; 10], tb: u32) -> Self {
		Cf {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_d: b'#',
			vs,
			_c: b'*',
			tb,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::vs::*;
	use std::ascii::escape_default;

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Cf::new(
			1,
			2,
			try_u128_to_versionstamp(12345).unwrap(),
			3,
		);
		let enc = Cf::encode(&val).unwrap();
		println!("enc={}", show(&enc));
		let dec = Cf::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn versionstamp_conversions() {
		let a = u64_to_versionstamp(12345);
		let b = try_to_u64_be(a).unwrap();
		assert_eq!(12345, b);

		let a = try_u128_to_versionstamp(12345).unwrap();
		let b = to_u128_be(a);
		assert_eq!(12345, b);
	}

	fn show(bs: &[u8]) -> String {
		let mut visible = String::new();
		for &b in bs {
			let part: Vec<u8> = escape_default(b).collect();
			visible.push_str(std::str::from_utf8(&part).unwrap());
		}
		visible
	}
}
