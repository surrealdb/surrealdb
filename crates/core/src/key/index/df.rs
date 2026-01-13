//! Stores a marker for deferred indexes.
//!
//! Deferred indexes are indexes that continue to be updated in the background
//! after their initial build completes. This key stores a boolean indicating
//! whether the initial build phase has completed, allowing the index to be
//! properly restored after a server restart.
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};

/// Key structure for storing deferred index status.
///
/// The key format is: `/*{ns}*{db}*{tb}+{ix}!df`
///
/// The value stored at this key is a boolean indicating whether the initial
/// build phase of the deferred index has completed.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Df<'a> {
	__: u8,
	_a: u8,
	/// The namespace
	pub ns: &'a str,
	_b: u8,
	/// The database
	pub db: &'a str,
	_c: u8,
	/// The table
	pub tb: &'a str,
	_d: u8,
	/// The index name
	pub ix: &'a str,
	_e: u8,
	_f: u8,
	_g: u8,
}
impl_key!(Df<'a>);

impl Categorise for Df<'_> {
	fn categorise(&self) -> Category {
		Category::IndexDeferred
	}
}

impl<'a> Df<'a> {
	/// Create a new deferred index status key for the given namespace, database, table, and index.
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
			_f: b'd',
			_g: b'f',
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::{KeyDecode, KeyEncode};
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Df::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
		);
		let enc = Df::encode(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!df");

		let dec = Df::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
