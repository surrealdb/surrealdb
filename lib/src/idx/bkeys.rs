use crate::idx::btree::Payload;
use crate::kvs::Key;
use fst::{IntoStreamer, Map, MapBuilder, Streamer};
use radix_trie::{SubTrie, Trie, TrieCommon};
use serde::{de, ser, Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::io;

pub(super) trait BKeys: Display + Sized {
	fn with_key_val(key: Key, payload: Payload) -> Self;
	fn len(&self) -> usize;
	fn get(&self, key: &Key) -> Option<Payload>;
	fn collect_with_prefix(&self, prefix_key: &Key, res: &mut Vec<(Key, Payload)>);
	fn insert(&mut self, key: Key, payload: Payload);
	fn _remove(&mut self, key: Key);
	fn split_keys(&self) -> SplitKeys<Self>;
	fn get_child_idx(&self, searched_key: &Key) -> usize;
	fn compile(&mut self) {}
	fn debug<F>(&self, to_string: F)
	where
		F: Fn(Key) -> String;
}

pub(super) struct SplitKeys<BK>
where
	BK: BKeys,
{
	pub(super) left: BK,
	pub(super) right: BK,
	pub(super) median_idx: usize,
	pub(super) median_key: Key,
	pub(super) median_payload: Payload,
}

pub(super) struct FstKeys {
	map: Map<Vec<u8>>,
	additions: Trie<Key, Payload>,
	deletions: Trie<Key, bool>,
}

impl BKeys for FstKeys {
	fn with_key_val(key: Key, payload: Payload) -> Self {
		let mut builder = MapBuilder::memory();
		builder.insert(key, payload).unwrap();
		Self::from(builder)
	}

	fn len(&self) -> usize {
		self.map.len()
	}

	fn get(&self, key: &Key) -> Option<Payload> {
		self.map.get(key)
	}

	fn collect_with_prefix(&self, _prefix_key: &Key, _res: &mut Vec<(Key, Payload)>) {
		panic!("Not supported!")
	}

	fn insert(&mut self, key: Key, payload: Payload) {
		self.additions.insert(key, payload);
	}

	fn _remove(&mut self, key: Key) {
		self.additions.remove(&key);
		self.deletions.insert(key, true);
	}

	fn split_keys(&self) -> SplitKeys<Self> {
		let median_idx = self.map.len() / 2;
		let mut s = self.map.stream();
		let mut left = MapBuilder::memory();
		let mut n = median_idx;
		while n > 0 {
			if let Some((key, payload)) = s.next() {
				left.insert(key, payload).unwrap();
			}
			n -= 1;
		}
		let (median_key, median_payload) = s
			.next()
			.map_or_else(|| panic!("The median key/value should exist"), |(k, v)| (k.into(), v));
		let mut right = MapBuilder::memory();
		while let Some((key, value)) = s.next() {
			right.insert(key, value).unwrap();
		}
		SplitKeys {
			left: Self::try_from(left).unwrap(),
			right: Self::try_from(right).unwrap(),
			median_idx,
			median_key,
			median_payload,
		}
	}

	fn get_child_idx(&self, searched_key: &Key) -> usize {
		let searched_key = searched_key.as_slice();
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

	fn debug<F>(&self, to_string: F)
	where
		F: Fn(Key) -> String,
	{
		let mut s = String::new();
		let mut iter = self.map.stream();
		let mut start = true;
		while let Some((k, p)) = iter.next() {
			if !start {
				s.push(',');
			} else {
				start = false;
			}
			s.push_str(&format!("{}={}", to_string(k.to_vec()).as_str(), p));
		}
		debug!("FSTKeys[{}]", s);
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
			serializer.serialize_bytes(self.map.as_fst().as_bytes())
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

#[derive(Default)]
pub(super) struct TrieKeys {
	keys: Trie<Key, Payload>,
}

impl Serialize for TrieKeys {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let uncompressed = bincode::serialize(&self.keys).unwrap();
		let mut reader = uncompressed.as_slice();
		let mut compressed: Vec<u8> = Vec::new();
		{
			let mut wtr = snap::write::FrameEncoder::new(&mut compressed);
			io::copy(&mut reader, &mut wtr).expect("I/O operation failed");
		}
		serializer.serialize_bytes(&compressed)
	}
}

impl<'de> Deserialize<'de> for TrieKeys {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let compressed: Vec<u8> = Deserialize::deserialize(deserializer)?;
		let reader = compressed.as_slice();
		let mut uncompressed: Vec<u8> = Vec::new();
		{
			let mut rdr = snap::read::FrameDecoder::new(reader);
			io::copy(&mut rdr, &mut uncompressed).expect("I/O operation failed");
		}
		let keys: Trie<Vec<u8>, u64> = bincode::deserialize(&uncompressed).unwrap();
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
	fn with_key_val(key: Key, payload: Payload) -> Self {
		let mut trie_keys = Self {
			keys: Trie::default(),
		};
		trie_keys.insert(key, payload);
		trie_keys
	}

	fn len(&self) -> usize {
		self.keys.len()
	}

	fn get(&self, key: &Key) -> Option<Payload> {
		self.keys.get(key).copied()
	}

	fn collect_with_prefix(&self, prefix: &Key, res: &mut Vec<(Key, Payload)>) {
		if let Some(node) = self.keys.get_raw_descendant(prefix) {
			TrieKeys::collect_with_prefix_recursive(node, prefix, res);
		}
	}

	fn insert(&mut self, key: Key, payload: Payload) {
		self.keys.insert(key, payload);
	}

	fn _remove(&mut self, key: Key) {
		self.keys.remove(&key);
	}

	fn split_keys(&self) -> SplitKeys<Self> {
		let median_idx = self.keys.len() / 2;
		let mut s = self.keys.iter();
		let mut left = Trie::default();
		let mut n = median_idx;
		while n > 0 {
			if let Some((key, payload)) = s.next() {
				left.insert(key.clone(), *payload);
			}
			n -= 1;
		}
		let (median_key, median_payload) = s
			.next()
			.map_or_else(|| panic!("The median key/value should exist"), |(k, v)| (k.clone(), *v));
		let mut right = Trie::default();
		for (key, val) in s {
			right.insert(key.clone(), *val);
		}
		SplitKeys {
			left: Self::from(left),
			right: Self::from(right),
			median_idx,
			median_key,
			median_payload,
		}
	}

	fn get_child_idx(&self, searched_key: &Key) -> usize {
		let mut child_idx = 0;
		for key in self.keys.keys() {
			if searched_key.le(key) {
				break;
			}
			child_idx += 1;
		}
		child_idx
	}

	fn debug<F>(&self, to_string: F)
	where
		F: Fn(Key) -> String,
	{
		let mut s = String::new();
		let mut start = true;
		for (k, p) in self.keys.iter() {
			if !start {
				s.push(',');
			} else {
				start = false;
			}
			s.push_str(&format!("{}={}", to_string(k.to_vec()).as_str(), p));
		}
		debug!("TrieKeys[{}]", s);
	}
}

impl TrieKeys {
	fn collect_with_prefix_recursive(
		node: SubTrie<Key, Payload>,
		prefix: &Key,
		res: &mut Vec<(Key, Payload)>,
	) {
		if let Some(value) = node.value() {
			if let Some(node_key) = node.key() {
				if node_key.starts_with(prefix) {
					res.push((node_key.clone(), *value));
				}
			}
		}

		for children in node.children() {
			Self::collect_with_prefix_recursive(children, prefix, res);
		}
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
	use crate::kvs::Key;

	#[test]
	fn test_fst_keys_serde() {
		let key: Key = "a".as_bytes().into();
		let keys = FstKeys::with_key_val(key.clone(), 130);
		let buf = bincode::serialize(&keys).unwrap();
		let keys: FstKeys = bincode::deserialize(&buf).unwrap();
		assert_eq!(keys.get(&key), Some(130));
	}

	#[test]
	fn test_trie_keys_serde() {
		let key: Key = "a".as_bytes().into();
		let keys = TrieKeys::with_key_val(key.clone(), 130);
		let buf = bincode::serialize(&keys).unwrap();
		let keys: TrieKeys = bincode::deserialize(&buf).unwrap();
		assert_eq!(keys.get(&key), Some(130));
	}

	fn test_keys_additions<BK: BKeys>(mut keys: BK) {
		let terms = [
			"the", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "dog", "the", "fast",
			"fox", "jumped", "over", "the", "lazy", "dog",
		];
		let mut i = 1;
		for term in terms {
			let key: Key = term.into();
			keys.insert(key.clone(), i);
			keys.compile();
			assert_eq!(keys.get(&key), Some(i));
			i += 1;
		}
	}

	#[test]
	fn test_fst_keys_additions() {
		test_keys_additions(FstKeys::default())
	}

	#[test]
	fn test_trie_keys_additions() {
		test_keys_additions(TrieKeys::default())
	}

	#[test]
	fn test_tries_keys_collect_with_prefix() {
		let mut keys = TrieKeys::default();
		keys.insert("apple".into(), 1);
		keys.insert("applicant".into(), 2);
		keys.insert("application".into(), 3);
		keys.insert("applicative".into(), 4);
		keys.insert("banana".into(), 5);
		keys.insert("blueberry".into(), 6);
		keys.insert("the".into(), 7);
		keys.insert("these".into(), 11);
		keys.insert("theses".into(), 12);
		keys.insert("their".into(), 8);
		keys.insert("theirs".into(), 9);
		keys.insert("there".into(), 10);

		{
			let mut res = vec![];
			keys.collect_with_prefix(&"appli".into(), &mut res);
			assert_eq!(
				res,
				vec![("applicant".into(), 2), ("application".into(), 3), ("applicative".into(), 4)]
			);
		}

		{
			let mut res = vec![];
			keys.collect_with_prefix(&"the".into(), &mut res);
			assert_eq!(
				res,
				vec![
					("the".into(), 7),
					("their".into(), 8),
					("theirs".into(), 9),
					("there".into(), 10),
					("these".into(), 11),
					("theses".into(), 12)
				]
			);
		}

		{
			let mut res = vec![];
			keys.collect_with_prefix(&"blue".into(), &mut res);
			assert_eq!(res, vec![("blueberry".into(), 6)]);
		}

		{
			let mut res = vec![];
			keys.collect_with_prefix(&"apple".into(), &mut res);
			assert_eq!(res, vec![("apple".into(), 1)]);
		}

		{
			let mut res = vec![];
			keys.collect_with_prefix(&"zz".into(), &mut res);
			assert_eq!(res, vec![]);
		}
	}

	fn test_keys_split<BK: BKeys>(mut keys: BK) {
		keys.insert("a".into(), 1);
		keys.insert("b".into(), 2);
		keys.insert("c".into(), 3);
		keys.insert("d".into(), 4);
		keys.insert("e".into(), 5);
		keys.compile();
		let r = keys.split_keys();
		assert_eq!(r.median_payload, 3);
		let c: Key = "c".into();
		assert_eq!(r.median_key, c);
		assert_eq!(r.median_idx, 2);
		assert_eq!(r.left.len(), 2);
		assert_eq!(r.left.get(&"a".into()), Some(1));
		assert_eq!(r.left.get(&"b".into()), Some(2));
		assert_eq!(r.right.len(), 2);
		assert_eq!(r.right.get(&"d".into()), Some(4));
		assert_eq!(r.right.get(&"e".into()), Some(5));
	}

	#[test]
	fn test_fst_keys_split() {
		test_keys_split(FstKeys::default());
	}

	#[test]
	fn test_trie_keys_split() {
		test_keys_split(TrieKeys::default());
	}
}
