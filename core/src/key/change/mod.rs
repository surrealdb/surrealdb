/// Stores change feeds
use derive::Key;
use serde::{Deserialize, Serialize};

use crate::vs;

use crate::key::error::KeyCategory;
use crate::key::key_req::KeyRequirements;
use std::str;

// Cf stands for change feeds
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Cf<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_d: u8,
	// vs is the versionstamp of the change feed entry that is encoded in big-endian.
	// Use the to_u64_be function to convert it to a u128.
	pub vs: [u8; 10],
	_c: u8,
	pub tb: &'a str,
}

#[allow(unused)]
pub fn new<'a>(ns: &'a str, db: &'a str, ts: u64, tb: &'a str) -> Cf<'a> {
	Cf::new(ns, db, vs::u64_to_versionstamp(ts), tb)
}

#[allow(unused)]
pub fn versionstamped_key_prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = crate::key::database::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(b"#");
	k
}

#[allow(unused)]
pub fn versionstamped_key_suffix(tb: &str) -> Vec<u8> {
	let mut k: Vec<u8> = vec![];
	k.extend_from_slice(b"*");
	k.extend_from_slice(tb.as_bytes());
	// Without this, decoding fails with UnexpectedEOF errors
	k.extend_from_slice(&[0x00]);
	k
}

/// Returns the prefix for the whole database change feeds since the
/// specified versionstamp.
#[allow(unused)]
pub fn prefix_ts(ns: &str, db: &str, vs: vs::Versionstamp) -> Vec<u8> {
	let mut k = crate::key::database::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(b"#");
	k.extend_from_slice(&vs);
	k
}

/// Returns the prefix for the whole database change feeds
#[allow(unused)]
pub fn prefix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = crate::key::database::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(b"#");
	k
}

/// Returns the suffix for the whole database change feeds
#[allow(unused)]
pub fn suffix(ns: &str, db: &str) -> Vec<u8> {
	let mut k = crate::key::database::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(&[b'#', 0xff]);
	k
}

impl KeyRequirements for Cf<'_> {
	fn key_category(&self) -> KeyCategory {
		KeyCategory::ChangeFeed
	}
}

impl<'a> Cf<'a> {
	pub fn new(ns: &'a str, db: &'a str, vs: [u8; 10], tb: &'a str) -> Self {
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
			"test",
			"test",
			try_u128_to_versionstamp(12345).unwrap(),
			"test",
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
