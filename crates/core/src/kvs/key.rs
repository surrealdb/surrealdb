/// A trait for types which can be encoded as a kv-store key.
pub trait KeyEncode {
	fn encode(&self) -> Result<Vec<u8>, crate::err::Error> {
		let mut buf = Vec::new();
		self.encode_into(&mut buf)?;
		Ok(buf)
	}

	fn encode_owned(self) -> Result<Vec<u8>, crate::err::Error>
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
	fn encode_into(&self, buffer: &mut Vec<u8>) -> Result<(), crate::err::Error>;

	fn encode_owned_into(self, buffer: &mut Vec<u8>) -> Result<(), crate::err::Error>
	where
		Self: Sized,
	{
		self.encode_into(buffer)
	}
}

/// A trait for types which can be decoded from a kv-store key bytes.
pub trait KeyDecode<'a> {
	fn decode(bytes: &'a [u8]) -> Result<Self, crate::err::Error>
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
	fn decode_from_vec(bytes: Vec<u8>) -> Result<Self, crate::err::Error>
	where
		Self: Sized,
	{
		Self::decode(&bytes)
	}
}

/// Implements KeyEncode and KeyDecode uusing storekey and deserialize and serialize
/// implementations.
macro_rules! impl_key {
	($name:ident$(<$l:lifetime>)?) => {
		impl$(<$l>)? crate::kvs::KeyEncode for $name $(<$l>)?{
			fn encode(&self) -> Result<Vec<u8>, crate::err::Error> {
				Ok(storekey::serialize(self)?)
			}

			fn encode_into(&self, buffer: &mut Vec<u8>) -> Result<(), crate::err::Error> {
				Ok(storekey::serialize_into(buffer, self)?)
			}
		}

		impl_key!(@decode $name $(,$l)?);
	};

	(@decode $name:ident, $l:lifetime) => {
		impl<$l> crate::kvs::KeyDecode<$l> for $name<$l>{
			fn decode(bytes: &$l[u8]) -> Result<Self, crate::err::Error> {
				Ok(storekey::deserialize(bytes)?)
			}
		}
	};

	(@decode $name:ident) => {
		impl<'a> crate::kvs::KeyDecode<'a> for $name{
			fn decode(bytes: &'a[u8]) -> Result<Self, crate::err::Error> {
				Ok(storekey::deserialize(bytes)?)
			}
		}

		impl crate::kvs::KeyDecodeOwned for $name {
			fn decode_from_vec(bytes: Vec<u8>) -> Result<Self, crate::err::Error> {
				Ok(storekey::deserialize(bytes.as_slice())?)
			}
		}
	};
}
pub(crate) use impl_key;

impl KeyEncode for Vec<u8> {
	fn encode(&self) -> Result<Vec<u8>, crate::err::Error> {
		Ok(self.clone())
	}

	fn encode_owned(self) -> Result<Vec<u8>, crate::err::Error> {
		Ok(self)
	}

	fn encode_into(&self, buffer: &mut Vec<u8>) -> Result<(), crate::err::Error> {
		buffer.extend_from_slice(&self);
		Ok(())
	}

	fn encode_owned_into(self, buffer: &mut Vec<u8>) -> Result<(), crate::err::Error> {
		if buffer.is_empty() {
			// we can just move self into the buffer since there is no data.
			*buffer = self;
		} else {
			// we can't overwrite the buffer so instead copy self into it.
			buffer.extend_from_slice(&self);
		}
		Ok(())
	}
}

impl<K: KeyEncode> KeyEncode for &K {
	fn encode_into(&self, buffer: &mut Vec<u8>) -> Result<(), crate::err::Error> {
		(*self).encode_into(buffer)
	}
}

impl KeyEncode for &str {
	fn encode_into(&self, buffer: &mut Vec<u8>) -> Result<(), crate::err::Error> {
		buffer.extend_from_slice(self.as_bytes());
		Ok(())
	}
}

impl KeyEncode for &[u8] {
	fn encode_into(&self, buffer: &mut Vec<u8>) -> Result<(), crate::err::Error> {
		buffer.extend_from_slice(self);
		Ok(())
	}
}

impl<'a> KeyDecode<'a> for Vec<u8> {
	fn decode(bytes: &[u8]) -> Result<Self, crate::err::Error>
	where
		Self: Sized,
	{
		Ok(bytes.to_vec())
	}
}

impl KeyDecodeOwned for Vec<u8> {
	fn decode_from_vec(bytes: Vec<u8>) -> Result<Self, crate::err::Error> {
		Ok(bytes)
	}
}

impl<'a> KeyDecode<'a> for () {
	fn decode(_: &'a [u8]) -> Result<Self, crate::err::Error>
	where
		Self: Sized,
	{
		Ok(())
	}
}

impl KeyDecodeOwned for () {
	fn decode_from_vec(_: Vec<u8>) -> Result<Self, crate::err::Error>
	where
		Self: Sized,
	{
		Ok(())
	}
}
