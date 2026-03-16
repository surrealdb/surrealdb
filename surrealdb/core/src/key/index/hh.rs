//! Stores Hashed Vectors of an HNSW index
use std::borrow::Cow;
use std::fmt::Debug;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::trees::hnsw::docs::ElementHashedDocs;
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

#[derive(Debug, Clone, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Hh<'a> {
	__: u8,
	_a: u8,
	/// The namespace ID
	pub ns: NamespaceId,
	_b: u8,
	/// The database ID
	pub db: DatabaseId,
	_c: u8,
	/// The table name
	pub tb: Cow<'a, TableName>,
	_d: u8,
	/// The index ID
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
	/// The BLAKE3 hash of the vector
	pub hash: [u8; 32],
}

impl_kv_key_storekey!(Hh<'_> => ElementHashedDocs);

impl<'a> Hh<'a> {
	/// Creates a new Hh key
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		hash: [u8; 32],
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
			_g: b'h',
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
			let hash = vec.compute_hash();
			let val = Hh::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), hash);
			let enc = Hh::encode_key(&val).unwrap();
			assert_eq!(enc, expected, "{info}: {}", String::from_utf8_lossy(&enc));
		};
		test(
			SerializedVector::I16(vec![1, 2, 3]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00+\x00\x00\x00\x03!hh\x26\x28\xed\x7d\x3c\xb9\x18\xf7\x6d\xbd\xd6\xe6\xe6\xb0\x53\x8f\x27\x15\x19\xc4\x99\x7c\xd6\x14\x4a\x69\x93\x9a\xf9\xf6\x84\x5c",
			"i16",
		);

		test(
			SerializedVector::I32(vec![1, 2, 3]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00+\x00\x00\x00\x03!hh\xdf\x07\xf4\x90\x4a\x7c\xcb\x20\x3d\xc9\x35\xda\xe7\xba\xf4\xa4\xc1\xf0\xab\x92\x79\xa8\x63\xa6\x91\x09\xbe\x74\xf2\x60\x32\xef",
			"i32",
		);

		test(
			SerializedVector::I64(vec![1, 2, 3]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00+\x00\x00\x00\x03!hh\x00\x1a\x4c\xc3\xa7\xc5\xc7\x39\xdf\x75\x9d\xf2\xc0\x56\x3c\x82\x24\xd5\xca\x89\xbe\x7f\xba\xbd\x99\xcf\x56\x88\xd2\xa0\x49\x15",
			"i64",
		);

		test(
			SerializedVector::F32(vec![1.0, 2.0, 3.0]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00+\x00\x00\x00\x03!hh\x8e\xca\x85\xc9\x29\x2e\x3a\xba\xb7\xa9\x74\xe8\x36\x32\x18\x89\x29\x45\x9d\x08\xe7\x0b\x53\x77\x21\xc4\x91\x9e\x22\xab\x0a\x27",
			"f32",
		);

		test(
			SerializedVector::F64(vec![1.0, 2.0, 3.0]),
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\x00+\x00\x00\x00\x03!hh\x53\x59\xf1\xe1\x6c\xcb\x8b\x69\x45\xd9\xf9\x94\xa8\x81\x90\x29\xce\xf0\x85\xf1\xbf\x0c\xb5\x41\x76\xf7\x6d\x9f\x83\xb8\x1c\x29",
			"f64",
		);
	}

	#[test]
	fn test_key_size_constant() {
		let tb = TableName::from("testtb");

		let v1 = SerializedVector::F64(vec![1.0, 2.0, 3.0]);
		let h1 = v1.compute_hash();
		let k1 = Hh::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), h1);
		let enc1 = Hh::encode_key(&k1).unwrap();

		let v2 = SerializedVector::F64(vec![0.0; 1536]);
		let h2 = v2.compute_hash();
		let k2 = Hh::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), h2);
		let enc2 = Hh::encode_key(&k2).unwrap();

		let v3 = SerializedVector::F64(vec![0.0; 16384]); // very large vector
		let h3 = v3.compute_hash();
		let k3 = Hh::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), h3);
		let enc3 = Hh::encode_key(&k3).unwrap();

		assert_eq!(enc1.len(), enc2.len());
		assert_eq!(enc2.len(), enc3.len());
	}

	#[test]
	fn test_hash_uniqueness_across_vector_types() {
		let v1 = SerializedVector::F32(vec![1.0, 2.0, 3.0]);
		let v2 = SerializedVector::F64(vec![1.0, 2.0, 3.0]);

		let h1 = v1.compute_hash();
		let h2 = v2.compute_hash();

		assert_ne!(h1, h2, "F32 and F64 vectors with same values should have different hashes");
	}

	#[test]
	fn test_hash_determinism() {
		let vec = SerializedVector::F64(vec![1.0, 2.0, 3.0]);

		// Compute hash multiple times
		let h1 = vec.compute_hash();
		let h2 = vec.compute_hash();

		assert_eq!(h1, h2);
	}
}
