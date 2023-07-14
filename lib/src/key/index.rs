use crate::key::CHAR_INDEX;
use crate::sql::array::Array;
use crate::sql::id::Id;
use derive::Key;
use serde::{Deserialize, Serialize};
use std::ops::Range;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
struct Prefix<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
}

impl<'a> Prefix<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: CHAR_INDEX,
			ix,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
struct PrefixIds<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	pub fd: Array,
}

impl<'a> PrefixIds<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, fd: &Array) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: CHAR_INDEX,
			ix,
			fd: fd.to_owned(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Index<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	pub fd: Array,
	pub id: Option<Id>,
}

impl<'a> Index<'a> {
	pub fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
		fd: Array,
		id: Option<Id>,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: CHAR_INDEX,
			ix,
			fd,
			id,
		}
	}

	pub fn range(ns: &str, db: &str, tb: &str, ix: &str) -> Range<Vec<u8>> {
		let mut beg = Prefix::new(ns, db, tb, ix).encode().unwrap();
		beg.extend_from_slice(&[0x00]);
		let mut end = Prefix::new(ns, db, tb, ix).encode().unwrap();
		end.extend_from_slice(&[0xff]);
		beg..end
	}

	pub fn range_all_ids(ns: &str, db: &str, tb: &str, ix: &str, fd: &Array) -> (Vec<u8>, Vec<u8>) {
		let mut beg = PrefixIds::new(ns, db, tb, ix, fd).encode().unwrap();
		beg.extend_from_slice(&[0x00]);
		let mut end = PrefixIds::new(ns, db, tb, ix, fd).encode().unwrap();
		end.extend_from_slice(&[0xff]);
		(beg, end)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Index::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			vec!["testfd1", "testfd2"].into(),
			Some("testid".into()),
		);
		let enc = Index::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\0\xa4testix\0\0\0\0\x04testfd1\0\0\0\0\x04testfd2\0\x01\x01\0\0\0\x01testid\0"
		);

		let dec = Index::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
