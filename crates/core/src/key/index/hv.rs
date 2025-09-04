//! Stores Things of an HNSW index
use std::borrow::Cow;
use std::fmt::Debug;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::idx::trees::hnsw::docs::ElementDocs;
use crate::idx::trees::vector::SerializedVector;
use crate::kvs::KVKey;

#[derive(Debug, Clone, PartialEq, Encode, BorrowDecode)]
pub(crate) struct Hv<'a> {
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
	pub vec: Cow<'a, SerializedVector>,
}

impl KVKey for Hv<'_> {
	type ValueType = ElementDocs;
}

impl<'a> Hv<'a> {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a str,
		ix: &'a str,
		vec: &'a SerializedVector,
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
			_f: b'h',
			_g: b'v',
			vec: Cow::Borrowed(vec),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn key() {
		let binding = SerializedVector::I16(vec![2]);
		let val = Hv::new(NamespaceId(1), DatabaseId(2), "testtb", "testix", &binding);
		let enc = Hv::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+testix\0!hv\x06\x80\x02\x00",
			"{}",
			String::from_utf8_lossy(&enc)
		);
	}
}
