//! Stores vector to document mappings for a DiskANN index.

use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::trees::diskann::docs::DiskAnnElementDocs;
use crate::idx::trees::vector::SerializedVector;
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

/// Maps a full serialized vector to the DiskANN graph element and document set that own it.
#[derive(Clone, Debug, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Dq<'a> {
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

impl_kv_key_storekey!(Dq<'_> => DiskAnnElementDocs);

impl<'a> Dq<'a> {
	/// Creates the `!dq{vector}` key for exact vector-document resolution.
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
			_f: b'd',
			_g: b'q',
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
			let tb = TableName::from("testtb");
			let val = Dq::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), &vec);
			let enc = Dq::encode_key(&val).unwrap();
			assert_eq!(enc, expected, "{info}: {}", String::from_utf8_lossy(&enc));
			let dec: Dq<'_> = storekey::decode_borrow(&enc).unwrap();
			assert_eq!(dec, val, "{info}");
		};

		test(
			SerializedVector::F32(vec![1.0, 2.0, 3.0]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!dq\x01\x01\x01\x01\x03\x01\0\x01\0\x80\x3F\x01\0\x01\0\x01\0\x40\x01\0\x01\0\x40\x40\0",
			"f32",
		);

		test(
			SerializedVector::F16(vec![1, 2, 3]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!dq\x01\x01\x05\x03\x01\x01\x01\0\x02\x01\0\x03\x01\0\0",
			"f16",
		);

		test(
			SerializedVector::I8(vec![1, 2, 3]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!dq\x01\x01\x06\x03\x01\x01\x02\x03\0",
			"i8",
		);

		test(
			SerializedVector::U8(vec![1, 2, 3]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!dq\x01\x01\x07\x03\x01\x01\x02\x03\0",
			"u8",
		);
	}
}
