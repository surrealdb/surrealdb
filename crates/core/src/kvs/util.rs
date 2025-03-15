use std::{ops::Range, sync::Arc};

use revision::Revisioned;

use crate::err::Error;

use super::KeyEncode;

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

pub fn to_prefix_range<K: KeyEncode>(key: K) -> Result<Range<Vec<u8>>, Error> {
	let start = key.encode_owned()?;
	let mut end = start.clone();
	end.push(0xff);
	Ok(Range {
		start,
		end,
	})
}

/// Takes an iterator of byte slices and deserializes the byte slices to the expected type,
/// returning an error if any of the values fail to serialize.
pub fn deserialize_cache<'a, I, T>(iter: I) -> Result<Arc<[T]>, Error>
where
	T: Revisioned,
	I: Iterator<Item = &'a [u8]>,
{
	let mut buf = Vec::new();
	for mut slice in iter {
		buf.push(Revisioned::deserialize_revisioned(&mut slice)?)
	}
	Ok(Arc::from(buf))
}
