//! Stores Things of an HNSW index
use std::borrow::Cow;
use std::fmt::Debug;

use crate::catalog::IndexId;
use crate::idx::trees::hnsw::docs::ElementDocs;
use crate::idx::trees::vector::SerializedVector;
use crate::key::database::all::DatabaseRoot;
use crate::key::{impl_kv_key_storekey, key};
use crate::val::TableName;

key! {
	#[derive(Debug, Clone, PartialEq)]
	pub(crate) struct Hv<'a> {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'!',
		b'h',
		b'v',
		pub vec: Cow<'a, SerializedVector>,
	}
}

impl_kv_key_storekey!(Hv<'a> => ElementDocs);

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;

	#[test]
	fn test_key() {
		let test = |vec: SerializedVector, expected: &[u8], info: &str| {
			let tb = TableName::from("testtb");
			let val = Hv {
				prefix: DatabaseRoot {
					ns: NamespaceId(1),
					db: DatabaseId(2),
				},
				tb: Cow::Borrowed(&tb),
				ix: IndexId(3),
				vec: Cow::Borrowed(&vec),
			};
			let enc = Hv::encode_key(&val).unwrap();
			assert_eq!(enc.as_slice(), expected, "{info}: {}", String::from_utf8_lossy(&enc));
			let dec: Hv<'_> = storekey::decode_borrow(&enc).unwrap();
			assert_eq!(dec, val, "{info}");
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

		test(
			SerializedVector::F16(vec![1, 2, 3]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv\x01\x01\x05\x03\x01\x01\x01\0\x02\x01\0\x03\x01\0\0",
			"f16",
		);

		test(
			SerializedVector::I8(vec![1, 2, 3]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv\x01\x01\x06\x03\x01\x01\x02\x03\0",
			"i8",
		);

		test(
			SerializedVector::U8(vec![1, 2, 3]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv\x01\x01\x07\x03\x01\x01\x02\x03\0",
			"u8",
		);
	}
}
