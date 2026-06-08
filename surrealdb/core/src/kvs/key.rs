//! Key and value traits for the key-value store.

use std::fmt::Debug;

use anyhow::{Context, Result};
use roaring::{RoaringBitmap, RoaringTreemap};

/// KVKey is a trait that defines a key for the key-value store.
pub(crate) trait KVKey: Debug + Sized {
	/// The associated value type for this key.
	type ValueType: KVValue;

	/// Encodes the key into a byte vector.
	fn encode_key(&self) -> Result<Vec<u8>>;

	/// Returns the context the value decoder needs to reconstruct fields
	/// derived from the key. For most key types this is `()`; for
	/// `RecordKey` it is the `RecordId` used to inject the canonical `id`
	/// during record decode. Encode never needs the key (the value is
	/// self-describing on encode), only decode does.
	fn value_context(&self) -> <Self::ValueType as KVValue>::KeyContext;
}

/// Implement `KVKey` for `$t` with `$v` as the associated value type.
///
/// Compile-fails for value types whose `KVValue::KeyContext` is not `()`
/// (currently only `Record`, with `KeyContext = RecordId`) — those need a
/// hand-written impl that supplies the context.
macro_rules! impl_kv_key_storekey {
	($(<$($tt:tt)*>)? $t:ty => $v:ty) => {
		impl$(<$($tt)*>)? crate::kvs::KVKey for $t {
			type ValueType = $v;

			fn encode_key(&self) -> ::anyhow::Result<Vec<u8>>{
				Ok(::storekey::encode_vec(self).map_err(|_| crate::err::Error::Unencodable)?)
			}

			fn value_context(&self) -> <$v as crate::kvs::KVValue>::KeyContext {}
		}
	};
}
pub(crate) use impl_kv_key_storekey;

impl KVKey for Vec<u8> {
	type ValueType = Vec<u8>;

	#[inline]
	fn encode_key(&self) -> Result<Vec<u8>> {
		Ok(self.clone())
	}

	#[inline]
	fn value_context(&self) {}
}

impl KVKey for String {
	type ValueType = Vec<u8>;

	#[inline]
	fn encode_key(&self) -> Result<Vec<u8>> {
		Ok(self.as_bytes().to_vec())
	}

	#[inline]
	fn value_context(&self) {}
}

impl KVKey for &str {
	type ValueType = Vec<u8>;

	#[inline]
	fn encode_key(&self) -> Result<Vec<u8>> {
		Ok(self.as_bytes().to_vec())
	}

	#[inline]
	fn value_context(&self) {}
}

/// KVValue is a trait that defines a value for the key-value store.
///
/// `KeyContext` is the data the value decoder needs from the storage key
/// to reconstruct fields that aren't stored in the value bytes. For most
/// types this is `()`; for `Record` it is `RecordId`, used to splice the
/// canonical `id` back into the decoded object (`Record::kv_encode_value`
/// strips it).
pub(crate) trait KVValue {
	type KeyContext;

	/// Encodes the value into a byte vector.
	fn kv_encode_value(&self) -> Result<Vec<u8>>;

	/// Decodes the value from a byte slice, consuming `ctx` to recover
	/// any fields derived from the storage key (see [`KeyContext`]).
	fn kv_decode_value(bytes: &[u8], ctx: Self::KeyContext) -> Result<Self>
	where
		Self: Sized;
}

macro_rules! impl_kv_value_revisioned {
	($name:ident) => {
		impl crate::kvs::KVValue for $name {
			type KeyContext = ();

			#[inline]
			fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
				Ok(revision::to_vec(self)?)
			}

			#[inline]
			fn kv_decode_value(bytes: &[u8], _: ()) -> anyhow::Result<Self> {
				Ok(revision::from_slice(bytes)?)
			}
		}
	};
}
pub(crate) use impl_kv_value_revisioned;

impl KVValue for Vec<u8> {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(self.clone())
	}

	#[inline]
	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		Ok(bytes.to_vec())
	}
}

impl KVValue for String {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(self.as_bytes().to_vec())
	}

	#[inline]
	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		std::str::from_utf8(bytes).context("String bytes must be valid utf8").map(str::to_owned)
	}
}

impl KVValue for u64 {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(self.to_be_bytes().to_vec())
	}

	#[inline]
	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		let arr: [u8; 8] =
			bytes.try_into().map_err(|_| anyhow::anyhow!("u64 bytes must be 8 bytes"))?;
		Ok(u64::from_be_bytes(arr))
	}
}

impl KVValue for () {
	type KeyContext = ();

	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(Vec::new())
	}

	fn kv_decode_value(_bytes: &[u8], _: ()) -> Result<Self> {
		Ok(())
	}
}

impl KVValue for RoaringBitmap {
	type KeyContext = ();

	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut bytes = Vec::new();
		self.serialize_into(&mut bytes)?;
		Ok(bytes)
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		Ok(Self::deserialize_from(bytes)?)
	}
}

impl KVValue for RoaringTreemap {
	type KeyContext = ();

	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut bytes = Vec::new();
		self.serialize_into(&mut bytes)?;
		Ok(bytes)
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		Ok(Self::deserialize_from(bytes)?)
	}
}

#[cfg(test)]
mod tests {
	use rstest::rstest;

	use super::*;

	#[rstest]
	#[case::str("test", b"test".to_vec())]
	#[case::string(String::from("test"), b"test".to_vec())]
	#[case::vec(vec![1, 2, 3], vec![1, 2, 3])]
	fn test_kv_key_primitives(#[case] key: impl KVKey, #[case] expected: Vec<u8>) {
		let encoded = key.encode_key().unwrap();
		assert_eq!(encoded, expected);
	}

	#[rstest]
	#[case::u64(123_u64, vec![0, 0, 0, 0, 0, 0, 0, 123])]
	#[case::unit((), Vec::new())]
	#[case::vec(vec![1, 2, 3], vec![1, 2, 3])]
	#[case::string(String::from("test"), b"test".to_vec())]
	#[case::roaring_bitmap(RoaringBitmap::new(), vec![58, 48, 0, 0, 0, 0, 0, 0])]
	#[case::roaring_treemap(RoaringTreemap::new(), vec![0, 0, 0, 0, 0, 0, 0, 0])]
	fn test_kv_value_primitives(#[case] value: impl KVValue, #[case] expected: Vec<u8>) {
		let encoded = value.kv_encode_value().unwrap();
		assert_eq!(encoded, expected);
	}
}
