use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use fst::{IntoStreamer, Map, MapBuilder, Streamer};
use radix_trie::{Trie, TrieCommon};
use serde::{de, ser, Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::io::{BufReader, Read, Write};

pub(super) trait BKeys: Display + Sized {
	fn with_key_val<K>(key: K, value: u64) -> Self
	where
		K: AsRef<[u8]>;
	fn len(&self) -> usize;
	fn get<K>(&self, key: K) -> Option<u64>
	where
		K: AsRef<[u8]>;
	fn insert<K>(&mut self, key: K, value: u64)
	where
		K: AsRef<[u8]>;
	fn _remove<K>(&mut self, key: K)
	where
		K: AsRef<[u8]>;
	fn split_keys(&self) -> (usize, Self, Vec<u8>, u64, Self);
	fn get_child_idx<K>(&self, searched_key: &K) -> usize
	where
		K: AsRef<[u8]>;
	fn compile(&mut self) {}
}

#[derive(Debug)]
pub(super) struct FstKeys {
	map: Map<Vec<u8>>,
	additions: Trie<Vec<u8>, u64>,
	deletions: Trie<Vec<u8>, bool>,
}

impl BKeys for FstKeys {
	fn with_key_val<K>(key: K, value: u64) -> Self
	where
		K: AsRef<[u8]>,
	{
		let mut builder = MapBuilder::memory();
		builder.insert(key, value).unwrap();
		Self::from(builder)
	}

	fn len(&self) -> usize {
		self.map.len()
	}

	fn get<K>(&self, key: K) -> Option<u64>
	where
		K: AsRef<[u8]>,
	{
		self.map.get(key)
	}

	fn insert<K>(&mut self, key: K, value: u64)
	where
		K: AsRef<[u8]>,
	{
		self.additions.insert(key.as_ref().to_vec(), value);
	}

	fn _remove<K>(&mut self, key: K)
	where
		K: AsRef<[u8]>,
	{
		self.additions.remove(key.as_ref());
		self.deletions.insert(key.as_ref().to_vec(), true);
	}

	fn split_keys(&self) -> (usize, FstKeys, Vec<u8>, u64, FstKeys) {
		let median_idx = self.map.len() / 2;
		let mut s = self.map.stream();
		let mut left = MapBuilder::memory();
		let mut n = median_idx;
		while n > 0 {
			if let Some((key, value)) = s.next() {
				left.insert(key, value).unwrap();
			}
			n -= 1;
		}
		let (median_key, median_value) = s
			.next()
			.map_or_else(|| panic!("The median key/value should exist"), |(k, v)| (k.to_vec(), v));
		let mut right = MapBuilder::memory();
		while let Some((key, value)) = s.next() {
			right.insert(key, value).unwrap();
		}
		(
			median_idx,
			Self::try_from(left).unwrap(),
			median_key,
			median_value,
			Self::try_from(right).unwrap(),
		)
	}

	fn get_child_idx<K>(&self, searched_key: &K) -> usize
	where
		K: AsRef<[u8]>,
	{
		let searched_key = searched_key.as_ref();
		let mut stream = self.map.keys().into_stream();
		let mut child_idx = 0;
		while let Some(key) = stream.next() {
			if searched_key.le(key) {
				break;
			}
			child_idx += 1;
		}
		child_idx
	}

	/// Rebuilt the FST by incorporating the changes (additions and deletions)
	fn compile(&mut self) {
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

		self.map = Map::new(builder.into_inner().unwrap()).unwrap();
		self.additions = Default::default();
		self.deletions = Default::default();
	}
}

impl Default for FstKeys {
	fn default() -> Self {
		Self::try_from(MapBuilder::memory()).unwrap()
	}
}

impl From<MapBuilder<Vec<u8>>> for FstKeys {
	fn from(builder: MapBuilder<Vec<u8>>) -> Self {
		Self::from(builder.into_inner().unwrap())
	}
}

impl From<Vec<u8>> for FstKeys {
	fn from(bytes: Vec<u8>) -> Self {
		let map = Map::new(bytes).unwrap();
		Self {
			map,
			additions: Default::default(),
			deletions: Default::default(),
		}
	}
}

impl Serialize for FstKeys {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if !self.deletions.is_empty() || !self.additions.is_empty() {
			Err(ser::Error::custom("bkeys.compile() should be called prior serializing"))
		} else {
			serializer.serialize_bytes(&self.map.as_fst().as_bytes())
		}
	}
}

impl<'de> Deserialize<'de> for FstKeys {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let buf: Vec<u8> = Deserialize::deserialize(deserializer)?;
		Self::try_from(buf).map_err(de::Error::custom)
	}
}

impl Display for FstKeys {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		let mut s = self.map.stream();
		let mut start = true;
		while let Some((key, val)) = s.next() {
			let key = String::from_utf8_lossy(key);
			if start {
				start = false;
			} else {
				f.write_str(", ")?;
			}
			write!(f, "{}=>{}", key, val)?;
		}
		Ok(())
	}
}

#[derive(Debug, Default)]
pub(super) struct TrieKeys {
	keys: Trie<Vec<u8>, u64>,
}

impl Serialize for TrieKeys {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let uncompressed_data = bincode::serialize(&self.keys).unwrap();
		let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
		encoder.write_all(&uncompressed_data).unwrap();
		let compressed_data = encoder.finish().unwrap();
		serializer.serialize_bytes(&compressed_data)
	}
}

impl<'de> Deserialize<'de> for TrieKeys {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let compressed: Vec<u8> = Deserialize::deserialize(deserializer)?;
		let decoder = GzDecoder::new(compressed.as_slice());
		let mut buf_reader = BufReader::new(decoder);
		let mut decompressed_data = Vec::new();
		buf_reader.read_to_end(&mut decompressed_data).unwrap();
		let keys: Trie<Vec<u8>, u64> = bincode::deserialize(&decompressed_data).unwrap();
		Ok(Self {
			keys,
		})
	}
}

impl Display for TrieKeys {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		let mut start = true;
		for (key, val) in self.keys.iter() {
			let key = String::from_utf8_lossy(key);
			if start {
				start = false;
			} else {
				f.write_str(", ")?;
			}
			write!(f, "{}=>{}", key, val)?;
		}
		Ok(())
	}
}

impl BKeys for TrieKeys {
	fn with_key_val<K>(key: K, val: u64) -> Self
	where
		K: AsRef<[u8]>,
	{
		let mut trie_keys = Self {
			keys: Trie::default(),
		};
		trie_keys.insert(key, val);
		trie_keys
	}

	fn len(&self) -> usize {
		self.keys.len()
	}

	fn get<K>(&self, key: K) -> Option<u64>
	where
		K: AsRef<[u8]>,
	{
		self.keys.get(key.as_ref()).copied()
	}

	fn insert<K>(&mut self, key: K, val: u64)
	where
		K: AsRef<[u8]>,
	{
		self.keys.insert(key.as_ref().to_vec(), val);
	}

	fn _remove<K>(&mut self, key: K)
	where
		K: AsRef<[u8]>,
	{
		self.keys.remove(key.as_ref());
	}

	fn split_keys(&self) -> (usize, Self, Vec<u8>, u64, Self) {
		let median_idx = self.keys.len() / 2;
		let mut s = self.keys.iter();
		let mut left = Trie::default();
		let mut n = median_idx;
		while n > 0 {
			if let Some((key, val)) = s.next() {
				left.insert(key.clone(), *val);
			}
			n -= 1;
		}
		let (median_key, median_value) = s
			.next()
			.map_or_else(|| panic!("The median key/value should exist"), |(k, v)| (k.to_vec(), *v));
		let mut right = Trie::default();
		while let Some((key, val)) = s.next() {
			right.insert(key.clone(), *val);
		}
		(median_idx, Self::from(left), median_key, median_value, Self::from(right))
	}

	fn get_child_idx<K>(&self, searched_key: &K) -> usize
	where
		K: AsRef<[u8]>,
	{
		let searched_key = searched_key.as_ref();
		let mut child_idx = 0;
		for key in self.keys.keys() {
			if searched_key.le(key.as_slice()) {
				break;
			}
			child_idx += 1;
		}
		child_idx
	}
}

impl From<Trie<Vec<u8>, u64>> for TrieKeys {
	fn from(keys: Trie<Vec<u8>, u64>) -> Self {
		Self {
			keys,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::bkeys::{BKeys, FstKeys, TrieKeys};

	#[test]
	fn test_fst_keys_serde() {
		let keys = FstKeys::default();
		let buf = bincode::serialize(&keys).unwrap();
		let _: FstKeys = bincode::deserialize(&buf).unwrap();
	}

	#[test]
	fn test_trie_keys_serde() {
		let keys = TrieKeys::with_key_val("1", 1);
		let buf = bincode::serialize(&keys).unwrap();
		let _: TrieKeys = bincode::deserialize(&buf).unwrap();
	}
}
