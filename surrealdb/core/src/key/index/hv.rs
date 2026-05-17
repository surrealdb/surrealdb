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

	/// Roundtrip test: every `SerializedVector` variant must encode + decode
	/// to a byte-equal vector + preserve the embedded prefix bytes.
	///
	/// This replaces the prior byte-exact `assert_eq!` tests because the
	/// `revisioned` bump from rev=1 to rev=2 (driven by adding the new `I8`
	/// variant) intentionally changes the wire-format framing — the rev=1
	/// `\x01\x01<discr>` prefix collapses to `\x02<discr>` at rev=2. Locking
	/// in byte-exact comparisons would force a re-update on every `revisioned`
	/// minor that re-frames the envelope; locking in roundtrip identity
	/// protects the semantic contract (encode → bytes → decode === original).
	#[test]
	fn test_key_roundtrip() {
		let cases: &[(SerializedVector, &str)] = &[
			(SerializedVector::I8(vec![1, 2, 3]), "i8"),
			(SerializedVector::I16(vec![1, 2, 3]), "i16"),
			(SerializedVector::I32(vec![1, 2, 3]), "i32"),
			(SerializedVector::I64(vec![1, 2, 3]), "i64"),
			(SerializedVector::F32(vec![1.0, 2.0, 3.0]), "f32"),
			(SerializedVector::F64(vec![1.0, 2.0, 3.0]), "f64"),
		];

		for (vec, info) in cases {
			let tb = TableName::from("testtb");
			let val = Hv::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), vec);
			let enc = Hv::encode_key(&val).unwrap();

			// The fixed bytes that frame the key (path separators, table name,
			// `hv` discriminator) must round-trip identically across variants.
			// We check just the prefix here; the variant-specific tail is the
			// `revisioned` payload, whose framing is rev-dependent.
			let prefix: &[u8] = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03!hv";
			assert!(
				enc.starts_with(prefix),
				"{info}: encoded key missing fixed prefix; got {:?}",
				String::from_utf8_lossy(&enc),
			);

			// Decode must recover an `Hv` whose vector compares equal.
			let dec: Hv<'_> = storekey::decode_borrow(&enc).expect("decode_borrow");
			assert_eq!(
				dec.vec.as_ref(),
				vec,
				"{info}: roundtrip mismatch (encoded then decoded vector differs)"
			);
		}
	}
}
