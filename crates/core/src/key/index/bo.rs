//! Stores the offsets
use std::borrow::Cow;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::idx::docids::DocId;
use crate::idx::ft::offset::OffsetRecords;
use crate::idx::ft::search::terms::TermId;
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Bo<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, str>,
	_d: u8,
	pub ix: Cow<'a, str>,
	_e: u8,
	_f: u8,
	_g: u8,
	pub doc_id: DocId,
	pub term_id: TermId,
}

impl_kv_key_storekey!(Bo<'_> => OffsetRecords);

impl Categorise for Bo<'_> {
	fn categorise(&self) -> Category {
		Category::IndexOffset
	}
}

impl<'a> Bo<'a> {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		ix: &'a str,
		doc_id: DocId,
		term_id: TermId,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'+',
			ix: Cow::Borrowed(ix),
			_e: b'!',
			_f: b'b',
			_g: b'o',
			doc_id,
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
		let val = Bo::new(
			NamespaceId(1),
			DatabaseId(2),
			"testtb",
			"testix",
			1,2
		);
		let enc = Bo::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!bo\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x02"
		);
	}
}
