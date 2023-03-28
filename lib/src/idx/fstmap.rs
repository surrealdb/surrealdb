use fst::map::Keys;
use fst::{IntoStreamer, Map, MapBuilder, Streamer};
use radix_trie::{Trie, TrieCommon};
use serde::{de, ser, Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub(super) struct FstMap {
	map: Map<Vec<u8>>,
	bytes: Vec<u8>,
	additions: Trie<Vec<u8>, u64>,
	deletions: Trie<Vec<u8>, bool>,
}

impl FstMap {
	pub(super) fn new() -> Result<Self, fst::Error> {
		Self::try_from(MapBuilder::memory())
	}

	pub(super) fn with_key_value(key: Vec<u8>, value: u64) -> Result<Self, fst::Error> {
		let mut builder = MapBuilder::memory();
		builder.insert(key, value).unwrap();
		Self::try_from(builder)
	}

	pub(super) fn size(&self) -> usize {
		self.bytes.len()
	}

	pub(super) fn len(&self) -> usize {
		self.map.len()
	}

	pub(super) fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<u64> {
		self.map.get(key)
	}

	pub(super) fn insert(&mut self, key: Vec<u8>, value: u64) {
		self.additions.insert(key, value);
	}

	pub(super) fn _remove(&mut self, key: Vec<u8>) {
		self.additions.remove(&key);
		self.deletions.insert(key, true);
	}

	pub(super) fn split_keys(&self) -> (usize, FstMap, Vec<u8>, u64, FstMap) {
		let mut n = self.map.len() / 2;
		let mut s = self.map.stream();
		let mut left = MapBuilder::memory();
		while n > 0 {
			if let Some((key, value)) = s.next() {
				left.insert(key, value).unwrap();
			}
			n -= 1;
		}
		let Some((median_key, median_value)) = s.next() else {panic!()};
		let median_key = median_key.to_vec();
		let mut right = MapBuilder::memory();
		while let Some((key, value)) = s.next() {
			right.insert(key, value).unwrap();
		}
		(n, Self::try_from(left).unwrap(), median_key, median_value, Self::try_from(right).unwrap())
	}

	pub(super) fn key_stream(&self) -> Keys {
		self.map.keys().into_stream()
	}

	/// Rebuilt the FST by incorporating the changes (additions and deletions)
	pub(super) fn rebuild(&mut self) {
		if self.additions.is_empty() && self.deletions.is_empty() {
			return;
		}
		let mut existing_keys = self.map.stream();
		let mut new_keys = self.additions.iter();
		let mut current_existing = existing_keys.next();
		let mut current_new = new_keys.next();

		let mut builder = MapBuilder::memory();
		// We use a double iterator because the map as to be filled with sorted terms
		loop {
			match current_new {
				None => break,
				Some((new_key_vec, new_value)) => match current_existing {
					None => break,
					Some((existing_key_vec, existing_value)) => {
						if self.deletions.get(existing_key_vec).is_some()
							|| self.additions.get(existing_key_vec).is_some()
						{
							current_existing = existing_keys.next();
						} else if new_key_vec.as_slice().ge(existing_key_vec) {
							builder.insert(existing_key_vec, existing_value).unwrap();
							current_existing = existing_keys.next();
						} else {
							builder.insert(new_key_vec, *new_value).unwrap();
							current_new = new_keys.next();
						}
					}
				},
			};
		}

		// Insert any existing term left over
		while let Some((existing_key_vec, value)) = current_existing {
			if self.deletions.get(existing_key_vec).is_none()
				&& self.additions.get(existing_key_vec).is_none()
			{
				builder.insert(existing_key_vec, value).unwrap();
				current_existing = existing_keys.next();
			}
		}
		// Insert any new term left over
		while let Some((new_key_vec, value)) = current_new {
			builder.insert(new_key_vec, *value).unwrap();
			current_new = new_keys.next();
		}

		self.bytes = builder.into_inner().unwrap();
		self.map = Map::new(self.bytes.clone()).unwrap();
		self.additions = Default::default();
		self.deletions = Default::default();
	}
}

impl TryFrom<MapBuilder<Vec<u8>>> for FstMap {
	type Error = fst::Error;

	fn try_from(builder: MapBuilder<Vec<u8>>) -> Result<Self, Self::Error> {
		Self::try_from(builder.into_inner()?)
	}
}

impl TryFrom<Vec<u8>> for FstMap {
	type Error = fst::Error;

	fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
		let map = Map::new(bytes.clone())?;
		Ok(Self {
			map,
			bytes,
			additions: Default::default(),
			deletions: Default::default(),
		})
	}
}

impl Serialize for FstMap {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if !self.deletions.is_empty() || !self.additions.is_empty() {
			Err(ser::Error::custom("fstmap.rebuild() should be called prior serializing"))
		} else {
			serializer.serialize_bytes(&self.bytes)
		}
	}
}

impl<'de> Deserialize<'de> for FstMap {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let buf: Vec<u8> = Deserialize::deserialize(deserializer)?;
		Self::try_from(buf).map_err(|e| de::Error::custom(e.to_string()))
	}
}

impl Display for FstMap {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		let mut s = self.map.stream();
		while let Some((key, value)) = s.next() {
			write!(f, "{:?}=>{}", key, value)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::fstmap::FstMap;

	#[test]
	fn test_fst_map_serde() {
		let map = FstMap::new().unwrap();
		// Check serialization / deserialization
		let buf = serde_json::to_vec(&map).unwrap();
		let _: FstMap = serde_json::from_slice(&buf).unwrap();
	}
}
