//! Stores terms for term_ids
use crate::idx::ft::search::terms::TermId;
use crate::key::category::{Categorise, Category};
use crate::kvs::KVKey;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Bu<'a> {
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
	pub term_id: TermId,
}

impl KVKey for Bu<'_> {
	type ValueType = Vec<u8>;
}

impl Categorise for Bu<'_> {
	fn categorise(&self) -> Category {
		Category::IndexTerms
	}
}

impl<'a> Bu<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, term_id: TermId) -> Self {
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
			_f: b'b',
			_g: b'u',
			term_id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Bu::new(
			"testns",
			"testdb",
			"testtb",
			"testix",
			7
		);
		let enc = Bu::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*testns\0*testdb\0*testtb\0+testix\0!bu\0\0\0\0\0\0\0\x07");
	}
}
