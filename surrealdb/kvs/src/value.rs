//! Values stored against keys: the encoding contract the datastore layer and
//! every stored type agree on.

use anyhow::{Context as _, Result};
use roaring::{RoaringBitmap, RoaringTreemap};

/// KVValue is a trait that defines a value for the key-value store.
///
/// `KeyContext` is the data the value decoder needs from the storage key
/// to reconstruct fields that aren't stored in the value bytes. For most
/// types this is `()`; for `Record` it is `RecordId`, used to splice the
/// canonical `id` back into the decoded object (`Record::kv_encode_value`
/// strips it).
pub trait KVValue {
	type KeyContext;

	/// Encodes the value into a byte vector.
	fn kv_encode_value(&self) -> Result<Vec<u8>>;

	/// Decodes the value from a byte slice, consuming `ctx` to recover
	/// any fields derived from the storage key (see [`KeyContext`]).
	fn kv_decode_value(bytes: &[u8], ctx: Self::KeyContext) -> Result<Self>
	where
		Self: Sized;
}

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

/// Implements [`KVValue`] for a type whose value bytes are its revisioned
/// encoding.
#[macro_export]
macro_rules! impl_kv_value_revisioned {
	($name:ident) => {
		impl $crate::value::KVValue for $name {
			type KeyContext = ();

			#[inline]
			fn kv_encode_value(&self) -> ::anyhow::Result<Vec<u8>> {
				Ok(::revision::to_vec(self)?)
			}

			#[inline]
			fn kv_decode_value(bytes: &[u8], _: ()) -> ::anyhow::Result<Self> {
				Ok(::revision::from_slice(bytes)?)
			}
		}
	};
}

#[cfg(test)]
mod tests {
	use rstest::rstest;

	use super::*;

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
