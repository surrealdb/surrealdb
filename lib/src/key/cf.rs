use derive::Key;
use serde::{Deserialize, Serialize};

use crate::vs;

use std::str;

// Cf stands for change feeds
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Cf {
	__: u8,
	_a: u8,
	pub ns: String,
	_b: u8,
	pub db: String,
	_d: u8,
	_e: u8,
	_f: u8,
	// vs is the versionstamp of the change feed entry that is encoded in big-endian.
	// Use the to_u64_be function to convert it to a u128.
	pub vs: [u8; 10],
	_c: u8,
	pub tb: String,
}

#[allow(unused)]
pub fn new(ns: &str, db: &str, ts: u64, tb: &str) -> Cf {
	Cf::new(ns.to_string(), db.to_string(), vs::u64_to_versionstamp(ts), tb.to_string())
}

#[allow(unused)]
pub fn versionstamped_key_prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'c', b'f']);
	k
}

#[allow(unused)]
pub fn versionstamped_key_suffix(tb: &str) -> Vec<u8> {
	let mut k: Vec<u8> = vec![];
	k.extend_from_slice(&[b'*']);
	k.extend_from_slice(tb.as_bytes());
	// Without this, decoding fails with UnexpectedEOF errors
	k.extend_from_slice(&[0x00]);
	k
}

/// Returns the prefix for the whole database change feeds since the
/// specified versionstamp.
#[allow(unused)]
pub fn ts_prefix(ns: &str, db: &str, vs: vs::Versionstamp) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'c', b'f']);
	k.extend_from_slice(&vs);
	k
}

/// Returns the prefix for the whole database change feeds
#[allow(unused)]
pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'c', b'f', 0x00]);
	k
}

/// Returns the suffix for the whole database change feeds
#[allow(unused)]
pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = super::database::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'!', b'c', b'f', 0xff]);
	k
}

impl Cf {
	pub fn new(ns: String, db: String, vs: [u8; 10], tb: String) -> Cf {
		Cf {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_d: b'!',
			_e: b'c',
			_f: b'f',
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
			"test".to_string(),
			"test".to_string(),
			u128_to_versionstamp(12345),
			"test".to_string(),
		);
		let enc = Cf::encode(&val).unwrap();
		println!("enc={}", show(&enc));
		let dec = Cf::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn versionstamp_conversions() {
		let a = u64_to_versionstamp(12345);
		let b = to_u64_be(a);
		assert_eq!(12345, b);

		let a = u128_to_versionstamp(12345);
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
