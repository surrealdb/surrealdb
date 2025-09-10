//! Stores Doc list for each term
use std::borrow::Cow;

use roaring::RoaringTreemap;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::ft::search::terms::TermId;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Bc<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
	_d: u8,
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
	pub term_id: TermId,
}

impl_kv_key_storekey!(Bc<'_> => RoaringTreemap);

impl Categorise for Bc<'_> {
	fn categorise(&self) -> Category {
		Category::IndexTermDocList
	}
}

impl<'a> Bc<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: &'a str, ix: IndexId, term_id: TermId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'+',
			ix,
			_e: b'!',
			_f: b'b',
			_g: b'c',
			term_id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn key() {
		#[rustfmt::skip]
		let val = Bc::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			IndexId(3),
			7
		);
		let enc = Bc::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!bc\0\0\0\0\0\0\0\x07"
		);
	}
}
