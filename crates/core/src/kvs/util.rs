use std::ops::Range;
use std::sync::Arc;

use anyhow::Result;

use crate::kvs::{KVKey, KVValue};

/// Advances a key to the next value,
/// can be used to skip over a certain key.
pub fn advance_key(key: &mut [u8]) {
	for b in key.iter_mut().rev() {
		*b = b.wrapping_add(1);
		if *b != 0 {
			break;
		}
	}
}

pub fn to_prefix_range<K: KVKey>(key: K) -> Result<Range<Vec<u8>>> {
	let start = key.encode_key()?;
	let mut end = start.clone();
	end.push(0xff);
	Ok(Range {
		start,
		end,
	})
}

/// Takes an iterator of byte slices and deserializes the byte slices to the
/// expected type, returning an error if any of the values fail to serialize.
pub fn deserialize_cache<'a, I, T>(iter: I) -> Result<Arc<[T]>>
where
	T: KVValue,
	I: Iterator<Item = &'a [u8]>,
{
	let mut buf = Vec::new();
	for slice in iter {
		buf.push(T::kv_decode_value(slice.to_vec())?)
	}
	Ok(Arc::from(buf))
}
