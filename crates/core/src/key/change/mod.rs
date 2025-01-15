//! Stores change feeds
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::vs::VersionStamp;
use derive::Key;
use serde::{Deserialize, Serialize};
use std::str;

// Cf stands for change feeds
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
#[non_exhaustive]
pub struct Cf<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_d: u8,
	// vs is the versionstamp of the change feed entry that is encoded in big-endian.
	pub vs: VersionStamp,
	_c: u8,
	pub tb: &'a str,
}

#[allow(unused)]
pub fn new<'a>(ns: &'a str, db: &'a str, ts: u64, tb: &'a str) -> Cf<'a> {
	Cf::new(ns, db, VersionStamp::from_u64(ts), tb)
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
pub fn prefix_ts(ns: &str, db: &str, vs: VersionStamp) -> Vec<u8> {
	let mut k = crate::key::database::all::new(ns, db).encode().unwrap();
	k.extend_from_slice(b"#");
	k.extend_from_slice(&vs.as_bytes());
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

impl Categorise for Cf<'_> {
	fn categorise(&self) -> Category {
		Category::ChangeFeed
	}
}

impl<'a> Cf<'a> {
	pub fn new(ns: &'a str, db: &'a str, vs: VersionStamp, tb: &'a str) -> Self {
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
			VersionStamp::try_from_u128(12345).unwrap(),
			"test",
		);
		let enc = Cf::encode(&val).unwrap();
		println!("enc={}", show(&enc));
		let dec = Cf::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn versionstamp_conversions() {
		let a = VersionStamp::from_u64(12345);
		let b = VersionStamp::try_into_u64(a).unwrap();
		assert_eq!(12345, b);

		let a = VersionStamp::try_from_u128(12345).unwrap();
		let b = a.into_u128();
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
