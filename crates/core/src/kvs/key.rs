use anyhow::{Context, Result};
use roaring::{RoaringBitmap, RoaringTreemap};
use std::fmt::Debug;

pub trait KVKey: serde::Serialize + Debug {
	type ValueType: KVValue;

	#[inline]
	fn encode_key(&self) -> ::std::result::Result<Vec<u8>, ::anyhow::Error> {
		Ok(storekey::serialize(self)?)
	}
}

impl KVKey for Vec<u8> {
	type ValueType = Vec<u8>;
}

impl KVKey for String {
	type ValueType = Vec<u8>;
}

impl KVKey for &str {
	type ValueType = Vec<u8>;
}

pub trait KVValue {
	fn kv_encode_value(&self) -> Result<Vec<u8>>;
	fn kv_decode_value(bytes: Vec<u8>) -> Result<Self>
	where
		Self: Sized;
}

macro_rules! impl_kv_value_revisioned {
	($name:ident) => {
		impl crate::kvs::KVValue for $name {
			fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
				Ok(revision::to_vec(self)?)
			}

			fn kv_decode_value(bytes: Vec<u8>) -> anyhow::Result<Self> {
				Ok(revision::from_slice(&bytes)?)
			}
		}
	};
}
pub(crate) use impl_kv_value_revisioned;

impl KVValue for Vec<u8> {
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(self.clone())
	}

	fn kv_decode_value(bytes: Vec<u8>) -> Result<Self> {
		Ok(bytes)
	}
}

impl KVValue for String {
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(self.as_bytes().to_vec())
	}

	fn kv_decode_value(bytes: Vec<u8>) -> Result<Self> {
		String::from_utf8(bytes).context("String bytes must be valid utf8")
	}
}

impl KVValue for u64 {
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(self.to_be_bytes().to_vec())
	}

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

/// A trait for types which can be encoded as a kv-store key.
pub trait KeyEncode {
	fn encode(&self) -> Result<Vec<u8>> {
		let mut buf = Vec::new();
		self.encode_into(&mut buf)?;
		Ok(buf)
	}

	fn encode_owned(self) -> Result<Vec<u8>>
	where
		Self: Sized,
	{
		self.encode()
	}

	/// Push the bytes this key would encode into the buffer.
	///
	/// Implementation can make no assumption about the contents of the buffer.
	/// The buffer should not be cleared and if there are bytes present in the buffer they should
	/// also be present when this function returns.
	fn encode_into(&self, buffer: &mut Vec<u8>) -> Result<()>;
}

/// A trait for types which can be decoded from a kv-store key bytes.
pub trait KeyDecode<'a> {
	fn decode(bytes: &'a [u8]) -> Result<Self>
	where
		Self: Sized;
}

pub trait KeyDecodeOwned: for<'a> KeyDecode<'a> {
	/// Decode the key from an owned vector.
	///
	/// A lot of kv query methods return vectors for keys, which some key types might be able to
	/// use to more effeciently decode the data.
	///
	/// The default implementation just calls decode
	fn decode_from_vec(bytes: Vec<u8>) -> Result<Self>
	where
		Self: Sized,
	{
		Self::decode(&bytes)
	}
}

impl KeyEncode for Vec<u8> {
	fn encode(&self) -> Result<Vec<u8>> {
		Ok(self.clone())
	}

	fn encode_owned(self) -> Result<Vec<u8>> {
		Ok(self)
	}

	fn encode_into(&self, buffer: &mut Vec<u8>) -> Result<()> {
		buffer.extend_from_slice(self);
		Ok(())
	}
}

impl<K: KeyEncode> KeyEncode for &K {
	fn encode_into(&self, buffer: &mut Vec<u8>) -> Result<()> {
		(*self).encode_into(buffer)
	}
}

impl KeyEncode for &str {
	fn encode_into(&self, buffer: &mut Vec<u8>) -> Result<()> {
		buffer.extend_from_slice(self.as_bytes());
		Ok(())
	}
}

impl KeyEncode for &[u8] {
	fn encode_into(&self, buffer: &mut Vec<u8>) -> Result<()> {
		buffer.extend_from_slice(self);
		Ok(())
	}
}

impl KeyDecode<'_> for Vec<u8> {
	fn decode(bytes: &[u8]) -> Result<Self>
	where
		Self: Sized,
	{
		Ok(bytes.to_vec())
	}
}

impl KeyDecodeOwned for Vec<u8> {
	fn decode_from_vec(bytes: Vec<u8>) -> Result<Self> {
		Ok(bytes)
	}
}

impl<'a> KeyDecode<'a> for () {
	fn decode(_: &'a [u8]) -> Result<Self>
	where
		Self: Sized,
	{
		Ok(())
	}
}

impl KeyDecodeOwned for () {
	fn decode_from_vec(_: Vec<u8>) -> Result<Self>
	where
		Self: Sized,
	{
		Ok(())
	}
}

/// Implements KeyEncode and KeyDecode uusing storekey and deserialize and serialize
/// implementations.
macro_rules! impl_key {
	($name:ident$(<$l:lifetime>)?) => {
		impl$(<$l>)? crate::kvs::KeyEncode for $name $(<$l>)?{
			fn encode(&self) -> ::std::result::Result<Vec<u8>, ::anyhow::Error> {
				Ok(storekey::serialize(self)?)
			}

			fn encode_into(&self, buffer: &mut Vec<u8>) -> ::std::result::Result<(), ::anyhow::Error> {
				Ok(storekey::serialize_into(buffer, self)?)
			}
		}

		impl_key!(@decode $name $(,$l)?);
	};

	(@decode $name:ident, $l:lifetime) => {
		impl<$l> crate::kvs::KeyDecode<$l> for $name<$l>{
			fn decode(bytes: &$l[u8]) -> ::std::result::Result<Self, ::anyhow::Error> {
				Ok(storekey::deserialize(bytes)?)
			}
		}
	};

	(@decode $name:ident) => {
		impl<'a> crate::kvs::KeyDecode<'a> for $name{
			fn decode(bytes: &'a[u8]) -> ::std::result::Result<Self, ::anyhow::Error> {
				Ok(storekey::deserialize(bytes)?)
			}
		}

		impl crate::kvs::KeyDecodeOwned for $name {
			fn decode_from_vec(bytes: Vec<u8>) -> ::std::result::Result<Self, ::anyhow::Error> {
				Ok(storekey::deserialize(bytes.as_slice())?)
			}
		}
	};
}
pub(crate) use impl_key;
