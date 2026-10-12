//! Values stored against keys: the encoding contract the datastore layer and
//! every stored type agree on.

use anyhow::{Context as _, Result};
use roaring::{RoaringBitmap, RoaringTreemap};

use crate::release::{FeatureLevel, Release};

/// What a levelled write needs: the store's feature level, and the bytes the write replaces.
#[derive(Clone, Copy, Debug)]
pub struct EncodeContext<'a> {
	/// The store's feature level. An encoding never exceeds what this level's release reads.
	pub level: FeatureLevel,
	/// The stored bytes the write replaces, when the key holds any. An encoder may carry forward
	/// from them what it does not itself understand.
	pub prior: Option<&'a [u8]>,
}

impl EncodeContext<'static> {
	/// A store at the Legacy level, replacing nothing.
	pub const LEGACY: Self = Self {
		level: FeatureLevel::Legacy,
		prior: None,
	};

	/// A store at this build's release, replacing nothing.
	pub const CURRENT: Self = Self {
		level: FeatureLevel::At(Release::CURRENT),
		prior: None,
	};
}

/// An encoded value, with the release floor of its stored-grammar form when one was written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Encoded {
	/// The value bytes.
	pub bytes: Vec<u8>,
	/// The oldest release able to read `bytes`, when `bytes` are in a grammar that records one.
	/// `None` for a fixed encoding, and for `revision`, which every release reads.
	pub floor: Option<Release>,
}

/// KVValue is a trait that defines a value for the key-value store.
///
/// `KeyContext` is the data the value decoder needs from the storage key
/// to reconstruct fields that aren't stored in the value bytes. For most
/// types this is `()`; for `Record` it is `RecordId`, used to splice the
/// canonical `id` back into the decoded object (`Record::kv_encode_value`
/// strips it).
pub trait KVValue {
	type KeyContext;

	/// Whether the encoding of this value depends on the store's feature level and on the bytes it
	/// replaces. False for every value whose encoding is fixed.
	///
	/// A writer of a levelled value must encode through [`Self::kv_encode_value_at`] with the
	/// store's level and the stored bytes being replaced, so that:
	///
	/// - the encoding is one the level's release reads;
	/// - what the replaced bytes hold that this build does not understand survives the write;
	/// - a write that needs a newer release than the level is refused instead of written
	///   unreadably.
	///
	/// A writer of a levelled value that compares before it writes must also compare through
	/// [`Self::kv_canonical`], because the replaced bytes may be in either of two grammars.
	/// [`Self::kv_encode_value`] is the encoding for callers that hold no store.
	const LEVELLED: bool = false;

	/// Encodes the value into a byte vector.
	fn kv_encode_value(&self) -> Result<Vec<u8>>;

	/// Encodes the value as a write at `ctx` would. A value with a fixed encoding returns
	/// [`Self::kv_encode_value`] with no floor.
	fn kv_encode_value_at(&self, ctx: &EncodeContext<'_>) -> Result<Encoded> {
		let _ = ctx;
		Ok(Encoded {
			bytes: self.kv_encode_value()?,
			floor: None,
		})
	}

	/// Bytes that are equal exactly when two values are equal, for compare-and-set. A value with
	/// more than one encoding of the same content overrides this with its canonical form. Defaults
	/// to [`Self::kv_encode_value`].
	fn kv_canonical(&self) -> Result<Vec<u8>> {
		self.kv_encode_value()
	}

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
