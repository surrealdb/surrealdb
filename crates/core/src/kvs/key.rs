//! Key and value traits for the key-value store.

use std::fmt::Debug;

use anyhow::{Context, Result};
use roaring::{RoaringBitmap, RoaringTreemap};

/// KVKey is a trait that defines a key for the key-value store.
pub trait KVKey: serde::Serialize + Debug + Sized {
	/// The associated value type for this key.
	type ValueType: KVValue;

	/// Encodes the key into a byte vector.
	#[inline]
	fn encode_key(&self) -> anyhow::Result<Vec<u8>> {
		Ok(storekey::serialize(self)?)
	}
}

impl KVKey for Vec<u8> {
	type ValueType = Vec<u8>;

	#[inline]
	fn encode_key(&self) -> anyhow::Result<Vec<u8>> {
		Ok(self.clone())
	}
}

impl KVKey for String {
	type ValueType = Vec<u8>;

	#[inline]
	fn encode_key(&self) -> anyhow::Result<Vec<u8>> {
		Ok(self.as_bytes().to_vec())
	}
}

impl KVKey for &str {
	type ValueType = Vec<u8>;

	#[inline]
	fn encode_key(&self) -> anyhow::Result<Vec<u8>> {
		Ok(self.as_bytes().to_vec())
	}
}

/// KVValue is a trait that defines a value for the key-value store.
pub trait KVValue {
	/// Encodes the value into a byte vector.
	fn kv_encode_value(&self) -> Result<Vec<u8>>;

	/// Decodes the value from a byte vector.
	fn kv_decode_value(bytes: Vec<u8>) -> Result<Self>
	where
		Self: Sized;
}

macro_rules! impl_kv_value_revisioned {
	($name:ident) => {
		impl crate::kvs::KVValue for $name {
			#[inline]
			fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
				Ok(revision::to_vec(self)?)
			}

			#[inline]
			fn kv_decode_value(bytes: Vec<u8>) -> anyhow::Result<Self> {
				Ok(revision::from_slice(&bytes)?)
			}
		}
	};
}
pub(crate) use impl_kv_value_revisioned;

impl KVValue for Vec<u8> {
	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(self.clone())
	}

	#[inline]
	fn kv_decode_value(bytes: Vec<u8>) -> Result<Self> {
		Ok(bytes)
	}
}

impl KVValue for String {
	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(self.as_bytes().to_vec())
	}

	#[inline]
	fn kv_decode_value(bytes: Vec<u8>) -> Result<Self> {
		String::from_utf8(bytes).context("String bytes must be valid utf8")
	}
}

impl KVValue for u64 {
	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(self.to_be_bytes().to_vec())
	}

	#[inline]
	fn kv_decode_value(bytes: Vec<u8>) -> Result<Self> {
		if bytes.len() != 8 {
			return Err(anyhow::anyhow!("u64 bytes must be 8 bytes"));
		}
		Ok(u64::from_be_bytes([
			bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
		]))
	}
}

impl KVValue for () {
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(Vec::new())
	}

	fn kv_decode_value(_bytes: Vec<u8>) -> Result<Self> {
		Ok(())
	}
}

impl KVValue for RoaringBitmap {
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut bytes = Vec::new();
		self.serialize_into(&mut bytes)?;
		Ok(bytes)
	}

	fn kv_decode_value(bytes: Vec<u8>) -> Result<Self> {
		Ok(Self::deserialize_from(&mut bytes.as_slice())?)
	}
}

impl KVValue for RoaringTreemap {
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut bytes = Vec::new();
		self.serialize_into(&mut bytes)?;
		Ok(bytes)
	}

	fn kv_decode_value(bytes: Vec<u8>) -> Result<Self> {
		Ok(Self::deserialize_from(&mut bytes.as_slice())?)
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
