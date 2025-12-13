//! Stores Things of an HNSW index
use std::borrow::Cow;
use std::fmt::Debug;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::trees::hnsw::docs::ElementDocs;
use crate::idx::trees::vector::SerializedVector;
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

#[derive(Debug, Clone, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Hv<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
	pub vec: Cow<'a, SerializedVector>,
}

impl_kv_key_storekey!(Hv<'_> => ElementDocs);

impl<'a> Hv<'a> {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
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
			ix,
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
	use crate::kvs::KVKey;

	#[test]
	fn test_key() {
		let test = |vec: SerializedVector, expected: &[u8], info: &str| {
			let val = Hv::new(NamespaceId(1), DatabaseId(2), "testtb", IndexId(3), &vec);
			let enc = Hv::encode_key(&val).unwrap();
			assert_eq!(enc, expected, "{info}: {}", String::from_utf8_lossy(&enc));
		};
		test(
			SerializedVector::I16(vec![1, 2, 3]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv\x01\x01\x04\x03\x01\x01\x01\0\x02\x01\0\x03\x01\0\0",
			"i16",
		);

		test(
			SerializedVector::I32(vec![1, 2, 3]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv\x01\x01\x03\x03\x01\x01\x01\0\x01\0\x01\0\x02\x01\0\x01\0\x01\0\x03\x01\0\x01\0\x01\0\0",
			"i32",
		);

		test(
			SerializedVector::I64(vec![1, 2, 3]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv\x01\x01\x02\x03\x01\x01\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\x02\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\x03\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\0",
			"i64",
		);

		test(
			SerializedVector::F32(vec![1.0, 2.0, 3.0]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv\x01\x01\x01\x01\x03\x01\0\x01\0\x80\x3F\x01\0\x01\0\x01\0\x40\x01\0\x01\0\x40\x40\0",
			"f32",
		);

		test(
			SerializedVector::F64(vec![1.0, 2.0, 3.0]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv\x01\x01\x01\0\x03\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\xF0\x3F\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\x40\x01\0\x01\0\x01\0\x01\0\x01\0\x01\0\x08\x40\0",
			"f64",
		);
	}
}
