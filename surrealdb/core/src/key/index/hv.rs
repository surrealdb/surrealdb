//! Stores Things of an HNSW index
use std::borrow::Cow;
use std::fmt::Debug;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::trees::hnsw::docs::ElementDocs;
use crate::idx::trees::vector::SerializedVectorHash;
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
	pub hash: SerializedVectorHash,
}

impl_kv_key_storekey!(Hv<'_> => ElementDocs);

impl<'a> Hv<'a> {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		hash: SerializedVectorHash,
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
			hash,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::idx::trees::vector::SerializedVector;
	use crate::kvs::KVKey;

	#[test]
	fn test_key() {
		let test = |vec: SerializedVector, expected: &[u8], info: &str| {
			let tb = TableName::from("testtb");
			let hash = SerializedVectorHash::from(&vec);
			let val = Hv::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), hash);
			let enc = Hv::encode_key(&val).unwrap();
			assert_eq!(enc, expected, "{info}: {}", String::from_utf8_lossy(&enc));
		};
		test(
			SerializedVector::I16(vec![1, 2, 3]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00+\x00\x00\x00\x03!hv&(\xed}<\xb9\x18\xf7m\xbd\xd6\xe6\xe6\xb0S\x8f'\x15\x19\xc4\x99|\xd6\x14Ji\x93\x9a\xf9\xf6\x84\\\x00",
			"i16",
		);

		test(
			SerializedVector::I32(vec![1, 2, 3]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00+\x00\x00\x00\x03!hv\xdf\x07\xf4\x90J|\xcb =\xc95\xda\xe7\xba\xf4\xa4\xc1\xf0\xab\x92y\xa8c\xa6\x91\t\xbet\xf2`2\xef\x00",
			"i32",
		);

		test(
			SerializedVector::I64(vec![1, 2, 3]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00+\x00\x00\x00\x03!hv\x01\x00\x1aL\xc3\xa7\xc5\xc79\xdfu\x9d\xf2\xc0V<\x82$\xd5\xca\x89\xbe\x7f\xba\xbd\x99\xcfV\x88\xd2\xa0I\x15\x00",
			"i64",
		);

		test(
			SerializedVector::F32(vec![1.0, 2.0, 3.0]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00+\x00\x00\x00\x03!hv\x8e\xca\x85\xc9).:\xba\xb7\xa9t\xe862\x18\x89)E\x9d\x08\xe7\x0bSw!\xc4\x91\x9e\"\xab\n\'\x00",
			"f32",
		);

		test(
			SerializedVector::F64(vec![1.0, 2.0, 3.0]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00+\x00\x00\x00\x03!hvSY\xf1\xe1l\xcb\x8biE\xd9\xf9\x94\xa8\x81\x90)\xce\xf0\x85\xf1\xbf\x0c\xb5Av\xf7m\x9f\x83\xb8\x1c)\x00",
			"f64",
		);
	}

	#[test]
	fn test_key_size_constant() {
		let tb = TableName::from("testtb");

		let v1 = SerializedVector::F64(vec![1.0, 2.0, 3.0]);
		let h1 = SerializedVectorHash::from(&v1);
		let k1 = Hv::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), h1);
		let enc1 = Hv::encode_key(&k1).unwrap();

		let v2 = SerializedVector::F64(vec![0.0; 1536]);
		let h2 = SerializedVectorHash::from(&v2);
		let k2 = Hv::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), h2);
		let enc2 = Hv::encode_key(&k2).unwrap();

		let v3 = SerializedVector::F64(vec![0.0; 16384]); // very large vector
		let h3 = SerializedVectorHash::from(&v3);
		let k3 = Hv::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), h3);
		let enc3 = Hv::encode_key(&k3).unwrap();

		assert_eq!(enc1.len(), enc2.len());
		assert_eq!(enc2.len(), enc3.len());
	}

	#[test]
	fn test_hash_uniqueness_across_vector_types() {
		let v1 = SerializedVector::F32(vec![1.0, 2.0, 3.0]);
		let v2 = SerializedVector::F64(vec![1.0, 2.0, 3.0]);

		let h1 = SerializedVectorHash::from(&v1);
		let h2 = SerializedVectorHash::from(&v2);

		assert_ne!(h1, h2, "F32 and F64 vectors with same values should have different hashes");
	}

	#[test]
	fn test_hash_determinism() {
		let vec = SerializedVector::F64(vec![1.0, 2.0, 3.0]);

		// Compute hash multiple times
		let h1 = SerializedVectorHash::from(&vec);
		let h2 = SerializedVectorHash::from(&vec);

		assert_eq!(h1, h2);
	}
}
