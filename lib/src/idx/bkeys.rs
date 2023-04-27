use crate::err::Error;
use crate::idx::btree::Payload;
use crate::kvs::{Key, Transaction};
use async_trait::async_trait;
use fst::{IntoStreamer, Map, MapBuilder, Streamer};
use radix_trie::{Trie, TrieCommon};
use serde::{de, ser, Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::io;

#[async_trait]
pub(super) trait KeyVisitor {
	async fn visit(
		&mut self,
		tx: &mut Transaction,
		key: Key,
		payload: Payload,
	) -> Result<(), Error>;
}

#[async_trait]
pub(super) trait BKeys: Display + Sized {
	fn with_key_val(key: Key, payload: Payload) -> Result<Self, Error>;
	fn len(&self) -> usize;
	fn get(&self, key: &Key) -> Option<Payload>;
	async fn collect_with_prefix<V>(
		&self,
		tx: &mut Transaction,
		prefix_key: &Key,
		visitor: &mut V,
	) -> Result<bool, Error>
	where
		V: KeyVisitor + Send;
	fn insert(&mut self, key: Key, payload: Payload);
	fn append(&mut self, keys: Self);
	fn remove(&mut self, key: &Key) -> Option<Payload>;
	fn split_keys(&self) -> SplitKeys<Self>;
	fn get_key(&self, idx: usize) -> Option<Key>;
	fn get_child_idx(&self, searched_key: &Key) -> usize;
	fn get_first_key(&self) -> Option<(Key, Payload)>;
	fn get_last_key(&self) -> Option<(Key, Payload)>;
	fn compile(&mut self) {}
	fn debug<F>(&self, to_string: F) -> Result<(), Error>
	where
		F: Fn(Key) -> Result<String, Error>;
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

#[derive(Default)]
pub(super) struct FstKeys {
	map: Map<Vec<u8>>,
	additions: Trie<Key, Payload>,
	deletions: Trie<Key, bool>,
	len: usize,
}

#[async_trait]
impl BKeys for FstKeys {
	fn with_key_val(key: Key, payload: Payload) -> Result<Self, Error> {
		let mut builder = MapBuilder::memory();
		builder.insert(key, payload).unwrap();
		Ok(Self::try_from(builder)?)
	}

	fn len(&self) -> usize {
		self.len
	}

	fn get(&self, key: &Key) -> Option<Payload> {
		if let Some(payload) = self.additions.get(key) {
			Some(*payload)
		} else {
			self.map.get(key).filter(|_| !self.deletions.get(key).is_some())
		}
	}

	async fn collect_with_prefix<V>(
		&self,
		_tx: &mut Transaction,
		_prefix_key: &Key,
		_visitor: &mut V,
	) -> Result<bool, Error>
	where
		V: KeyVisitor,
	{
		panic!("Not supported!")
	}

	fn insert(&mut self, key: Key, payload: Payload) {
		self.deletions.remove(&key);
		let existing_key = self.map.get(&key).is_some();
		if self.additions.insert(key, payload).is_none() {
			if !existing_key {
				self.len += 1;
			}
		}
	}

	fn append(&mut self, mut keys: Self) {
		keys.compile();
		let mut s = keys.map.stream();
		while let Some((key, payload)) = s.next() {
			self.insert(key.to_vec(), payload);
		}
	}

	fn remove(&mut self, key: &Key) -> Option<Payload> {
		if self.deletions.get(key).is_some() {
			return None;
		}
		if let Some(payload) = self.additions.remove(key) {
			self.len -= 1;
			return Some(payload);
		}
		self.get(key).map(|payload| {
			if self.deletions.insert(key.clone(), true).is_none() {
				self.len -= 1;
			}
			payload
		})
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

	fn get_key(&self, mut idx: usize) -> Option<Key> {
		let mut s = self.map.keys().into_stream();
		while let Some(key) = s.next() {
			if idx == 0 {
				return Some(key.to_vec());
			}
			idx -= 1;
		}
		None
	}

	fn get_child_idx(&self, searched_key: &Key) -> usize {
		let searched_key = searched_key.as_slice();
		let mut s = self.map.keys().into_stream();
		let mut child_idx = 0;
		while let Some(key) = s.next() {
			if searched_key.le(key) {
				break;
			}
			child_idx += 1;
		}
		child_idx
	}

	fn get_first_key(&self) -> Option<(Key, Payload)> {
		self.map.stream().next().map(|(k, p)| (k.to_vec(), p))
	}

	fn get_last_key(&self) -> Option<(Key, Payload)> {
		let mut last = None;
		let mut s = self.map.stream();
		while let Some((k, p)) = s.next() {
			last = Some((k.to_vec(), p));
		}
		last
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
			}
			current_existing = existing_keys.next();
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

	fn debug<F>(&self, to_string: F) -> Result<(), Error>
	where
		F: Fn(Key) -> Result<String, Error>,
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
			s.push_str(&format!("{}={}", to_string(k.to_vec())?.as_str(), p));
		}
		debug!("FSTKeys[{}]", s);
		Ok(())
	}
}

impl TryFrom<MapBuilder<Vec<u8>>> for FstKeys {
	type Error = fst::Error;
	fn try_from(builder: MapBuilder<Vec<u8>>) -> Result<Self, Self::Error> {
		Self::try_from(builder.into_inner()?)
	}
}

impl TryFrom<Vec<u8>> for FstKeys {
	type Error = fst::Error;
	fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
		let map = Map::new(bytes)?;
		let len = map.len();
		Ok(Self {
			map,
			len,
			additions: Default::default(),
			deletions: Default::default(),
		})
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

#[async_trait]
impl BKeys for TrieKeys {
	fn with_key_val(key: Key, payload: Payload) -> Result<Self, Error> {
		let mut trie_keys = Self {
			keys: Trie::default(),
		};
		trie_keys.insert(key, payload);
		Ok(trie_keys)
	}

	fn len(&self) -> usize {
		self.keys.len()
	}

	fn get(&self, key: &Key) -> Option<Payload> {
		self.keys.get(key).copied()
	}

	async fn collect_with_prefix<V>(
		&self,
		tx: &mut Transaction,
		prefix: &Key,
		visitor: &mut V,
	) -> Result<bool, Error>
	where
		V: KeyVisitor + Send,
	{
		let mut node_queue = VecDeque::new();
		if let Some(node) = self.keys.get_raw_descendant(prefix) {
			node_queue.push_front(node);
		}
		let mut found = false;
		while let Some(node) = node_queue.pop_front() {
			if let Some(value) = node.value() {
				if let Some(node_key) = node.key() {
					if node_key.starts_with(prefix) {
						found = true;
						visitor.visit(tx, node_key.clone(), *value).await?;
					}
				}
			}
			for children in node.children() {
				node_queue.push_front(children);
			}
		}
		Ok(found)
	}

	fn insert(&mut self, key: Key, payload: Payload) {
		self.keys.insert(key, payload);
	}

	fn append(&mut self, keys: Self) {
		for (k, p) in keys.keys.iter() {
			self.insert(k.clone(), *p);
		}
	}

	fn remove(&mut self, key: &Key) -> Option<Payload> {
		self.keys.remove(key)
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

	fn get_key(&self, mut idx: usize) -> Option<Key> {
		for key in self.keys.keys() {
			if idx == 0 {
				return Some(key.clone());
			}
			idx -= 1;
		}
		None
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

	fn get_first_key(&self) -> Option<(Key, Payload)> {
		self.keys.iter().next().map(|(k, p)| (k.clone(), *p))
	}

	fn get_last_key(&self) -> Option<(Key, Payload)> {
		self.keys.iter().last().map(|(k, p)| (k.clone(), *p))
	}

	fn debug<F>(&self, to_string: F) -> Result<(), Error>
	where
		F: Fn(Key) -> Result<String, Error>,
	{
		let mut s = String::new();
		let mut start = true;
		for (k, p) in self.keys.iter() {
			if !start {
				s.push(',');
			} else {
				start = false;
			}
			s.push_str(&format!("{}={}", to_string(k.to_vec())?.as_str(), p));
		}
		debug!("TrieKeys[{}]", s);
		Ok(())
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
	use crate::idx::tests::HashVisitor;
	use crate::kvs::{Datastore, Key};
	use std::collections::HashSet;

	#[test]
	fn test_fst_keys_serde() {
		let key: Key = "a".as_bytes().into();
		let keys = FstKeys::with_key_val(key.clone(), 130).unwrap();
		let buf = bincode::serialize(&keys).unwrap();
		let keys: FstKeys = bincode::deserialize(&buf).unwrap();
		assert_eq!(keys.get(&key), Some(130));
	}

	#[test]
	fn test_trie_keys_serde() {
		let key: Key = "a".as_bytes().into();
		let keys = TrieKeys::with_key_val(key.clone(), 130).unwrap();
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
		assert_eq!(keys.len(), 0);
		let mut term_set = HashSet::new();
		for term in terms {
			term_set.insert(term.to_string());
			let key: Key = term.into();
			keys.insert(key.clone(), i);
			keys.compile();
			assert_eq!(keys.get(&key), Some(i));
			assert_eq!(keys.len(), term_set.len());
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

	fn test_keys_deletions<BK: BKeys>(mut keys: BK) {
		assert_eq!(keys.remove(&"dummy".into()), None);
		assert_eq!(keys.len(), 0);
		keys.insert("foo".into(), 1);
		keys.insert("bar".into(), 2);
		assert_eq!(keys.len(), 2);
		assert_eq!(keys.remove(&"bar".into()), Some(2));
		assert_eq!(keys.len(), 1);
		assert_eq!(keys.remove(&"bar".into()), None);
		assert_eq!(keys.len(), 1);
		assert_eq!(keys.remove(&"foo".into()), Some(1));
		assert_eq!(keys.len(), 0);
		assert_eq!(keys.remove(&"foo".into()), None);
		assert_eq!(keys.len(), 0);
	}

	#[test]
	fn test_fst_keys_deletions() {
		test_keys_deletions(FstKeys::default())
	}

	#[test]
	fn test_trie_keys_deletions() {
		test_keys_deletions(TrieKeys::default())
	}

	#[tokio::test]
	async fn test_tries_keys_collect_with_prefix() {
		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();

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
			let mut visitor = HashVisitor::default();
			keys.collect_with_prefix(&mut tx, &"appli".into(), &mut visitor).await.unwrap();
			visitor.check(
				vec![("applicant".into(), 2), ("application".into(), 3), ("applicative".into(), 4)],
				"appli",
			);
		}

		{
			let mut visitor = HashVisitor::default();
			keys.collect_with_prefix(&mut tx, &"the".into(), &mut visitor).await.unwrap();
			visitor.check(
				vec![
					("the".into(), 7),
					("their".into(), 8),
					("theirs".into(), 9),
					("there".into(), 10),
					("these".into(), 11),
					("theses".into(), 12),
				],
				"the",
			);
		}

		{
			let mut visitor = HashVisitor::default();
			keys.collect_with_prefix(&mut tx, &"blue".into(), &mut visitor).await.unwrap();
			visitor.check(vec![("blueberry".into(), 6)], "blue");
		}

		{
			let mut visitor = HashVisitor::default();
			keys.collect_with_prefix(&mut tx, &"apple".into(), &mut visitor).await.unwrap();
			visitor.check(vec![("apple".into(), 1)], "apple");
		}

		{
			let mut visitor = HashVisitor::default();
			keys.collect_with_prefix(&mut tx, &"zz".into(), &mut visitor).await.unwrap();
			visitor.check(vec![], "zz");
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
