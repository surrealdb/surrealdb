use std::collections::VecDeque;
use std::fmt::{Debug, Display, Formatter};
use std::io;
use std::io::Cursor;

use anyhow::Result;
use fst::{IntoStreamer, Map, MapBuilder, Streamer};
use radix_trie::{SubTrie, Trie, TrieCommon};
use serde::ser;

use crate::err::Error;
use crate::idx::trees::btree::Payload;
use crate::kvs::Key;

pub trait BKeys: Default + Debug + Display + Sized {
	fn with_key_val(key: Key, payload: Payload) -> Result<Self>;
	fn len(&self) -> u32;
	fn is_empty(&self) -> bool;
	fn get(&self, key: &Key) -> Option<Payload>;
	// It is okay to return a owned Vec rather than an iterator,
	// because BKeys are intended to be stored as Node in the BTree.
	// The size of the Node should be small, therefore one instance of
	// BKeys would never be store a large volume of keys.
	fn collect_with_prefix(&self, prefix_key: &Key) -> Result<VecDeque<(Key, Payload)>>;
	fn insert(&mut self, key: Key, payload: Payload) -> Option<Payload>;
	fn append(&mut self, keys: Self);
	fn remove(&mut self, key: &Key) -> Option<Payload>;
	fn split_keys(self) -> Result<SplitKeys<Self>>;
	fn get_key(&self, idx: usize) -> Option<Key>;
	fn get_child_idx(&self, searched_key: &Key) -> usize;
	fn get_first_key(&self) -> Option<(Key, Payload)>;
	fn get_last_key(&self) -> Option<(Key, Payload)>;
	fn read_from(c: &mut Cursor<Vec<u8>>) -> Result<Self>;
	fn write_to(&self, c: &mut Cursor<Vec<u8>>) -> Result<()>;
	fn compile(&mut self) {}
}

pub struct SplitKeys<BK>
where
	BK: BKeys,
{
	pub(in crate::idx) left: BK,
	pub(in crate::idx) right: BK,
	pub(in crate::idx) median_idx: usize,
	pub(in crate::idx) median_key: Key,
	pub(in crate::idx) median_payload: Payload,
}

#[derive(Debug, Clone)]
pub struct FstKeys {
	i: Inner,
}

#[derive(Debug, Clone)]
enum Inner {
	Map(Map<Vec<u8>>),
	Trie(TrieKeys),
}

impl FstKeys {
	fn edit(&mut self) {
		if let Inner::Map(m) = &self.i {
			let t: TrieKeys = m.into();
			self.i = Inner::Trie(t);
		}
	}
}

impl Default for FstKeys {
	fn default() -> Self {
		Self {
			i: Inner::Trie(TrieKeys::default()),
		}
	}
}

impl BKeys for FstKeys {
	fn with_key_val(key: Key, payload: Payload) -> Result<Self> {
		let i = Inner::Trie(TrieKeys::with_key_val(key, payload)?);
		Ok(Self {
			i,
		})
	}

	fn len(&self) -> u32 {
		match &self.i {
			Inner::Map(m) => m.len() as u32,
			Inner::Trie(t) => t.len(),
		}
	}

	fn is_empty(&self) -> bool {
		match &self.i {
			Inner::Map(m) => m.is_empty(),
			Inner::Trie(t) => t.is_empty(),
		}
	}

	fn get(&self, key: &Key) -> Option<Payload> {
		match &self.i {
			Inner::Map(m) => m.get(key),
			Inner::Trie(t) => t.get(key),
		}
	}

	fn collect_with_prefix(&self, _prefix_key: &Key) -> Result<VecDeque<(Key, Payload)>> {
		fail!("BKeys/FSTKeys::collect_with_prefix")
	}

	fn insert(&mut self, key: Key, payload: Payload) -> Option<Payload> {
		self.edit();
		if let Inner::Trie(t) = &mut self.i {
			return t.insert(key, payload);
		}
		unreachable!()
	}

	fn append(&mut self, keys: Self) {
		if keys.is_empty() {
			return;
		}
		self.edit();
		match keys.i {
			Inner::Map(other) => {
				let mut s = other.stream();
				while let Some((key, payload)) = s.next() {
					self.insert(key.to_vec(), payload);
				}
			}
			Inner::Trie(other) => {
				if let Inner::Trie(t) = &mut self.i {
					t.append(other)
				}
			}
		}
	}

	fn remove(&mut self, key: &Key) -> Option<Payload> {
		self.edit();
		if let Inner::Trie(t) = &mut self.i {
			t.remove(key)
		} else {
			None
		}
	}

	fn split_keys(mut self) -> Result<SplitKeys<Self>> {
		self.edit();
		if let Inner::Trie(t) = self.i {
			let s = t.split_keys()?;
			Ok(SplitKeys {
				left: Self {
					i: Inner::Trie(s.left),
				},
				right: Self {
					i: Inner::Trie(s.right),
				},
				median_idx: s.median_idx,
				median_key: s.median_key,
				median_payload: s.median_payload,
			})
		} else {
			fail!("BKeys/FSTKeys::split_keys")
		}
	}

	fn get_key(&self, mut idx: usize) -> Option<Key> {
		match &self.i {
			Inner::Map(m) => {
				let mut s = m.keys().into_stream();
				while let Some(key) = s.next() {
					if idx == 0 {
						return Some(key.to_vec());
					}
					idx -= 1;
				}
				None
			}
			Inner::Trie(t) => t.get_key(idx),
		}
	}

	fn get_child_idx(&self, searched_key: &Key) -> usize {
		match &self.i {
			Inner::Map(m) => {
				let searched_key = searched_key.as_slice();
				let mut s = m.keys().into_stream();
				let mut child_idx = 0;
				while let Some(key) = s.next() {
					if searched_key.le(key) {
						break;
					}
					child_idx += 1;
				}
				child_idx
			}
			Inner::Trie(t) => t.get_child_idx(searched_key),
		}
	}

	fn get_first_key(&self) -> Option<(Key, Payload)> {
		match &self.i {
			Inner::Map(m) => m.stream().next().map(|(k, p)| (k.to_vec(), p)),
			Inner::Trie(t) => t.get_first_key(),
		}
	}

	fn get_last_key(&self) -> Option<(Key, Payload)> {
		match &self.i {
			Inner::Map(m) => {
				let mut last = None;
				let mut s = m.stream();
				while let Some((k, p)) = s.next() {
					last = Some((k.to_vec(), p));
				}
				last
			}
			Inner::Trie(t) => t.get_last_key(),
		}
	}

	fn compile(&mut self) {
		if let Inner::Trie(t) = &self.i {
			let mut builder = MapBuilder::memory();
			for (key, payload) in t.keys.iter() {
				builder.insert(key, *payload).unwrap();
			}
			let m = Map::new(builder.into_inner().unwrap()).unwrap();
			self.i = Inner::Map(m);
		}
	}

	fn read_from(c: &mut Cursor<Vec<u8>>) -> Result<Self> {
		let bytes: Vec<u8> = bincode::deserialize_from(c)?;
		Ok(Self::try_from(bytes)?)
	}

	fn write_to(&self, c: &mut Cursor<Vec<u8>>) -> Result<()> {
		if let Inner::Map(m) = &self.i {
			let b = m.as_fst().as_bytes();
			bincode::serialize_into(c, b)?;
			Ok(())
		} else {
			Err(anyhow::Error::new(Error::Bincode(ser::Error::custom(
				"bkeys.to_map() should be called prior serializing",
			))))
		}
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
		Ok(Self {
			i: Inner::Map(map),
		})
	}
}

impl Display for FstKeys {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match &self.i {
			Inner::Map(m) => {
				let mut s = m.stream();
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
			Inner::Trie(t) => write!(f, "{}", t),
		}
	}
}

#[derive(Default, Debug, Clone)]
pub struct TrieKeys {
	keys: Trie<Key, Payload>,
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

impl From<&Map<Vec<u8>>> for TrieKeys {
	fn from(m: &Map<Vec<u8>>) -> Self {
		let mut keys = TrieKeys::default();
		let mut s = m.stream();
		while let Some((key, payload)) = s.next() {
			keys.insert(key.to_vec(), payload);
		}
		keys
	}
}

impl BKeys for TrieKeys {
	fn with_key_val(key: Key, payload: Payload) -> Result<Self> {
		let mut trie_keys = Self {
			keys: Trie::default(),
		};
		trie_keys.insert(key, payload);
		Ok(trie_keys)
	}

	fn len(&self) -> u32 {
		self.keys.len() as u32
	}

	fn is_empty(&self) -> bool {
		self.keys.is_empty()
	}

	fn get(&self, key: &Key) -> Option<Payload> {
		self.keys.get(key).copied()
	}

	fn collect_with_prefix(&self, prefix: &Key) -> Result<VecDeque<(Key, Payload)>> {
		let mut i = KeysIterator::new(prefix, &self.keys);
		let mut r = VecDeque::new();
		while let Some((k, p)) = i.next() {
			r.push_back((k.clone(), p))
		}
		Ok(r)
	}

	fn insert(&mut self, key: Key, payload: Payload) -> Option<Payload> {
		self.keys.insert(key, payload)
	}

	fn append(&mut self, keys: Self) {
		for (k, p) in keys.keys.iter() {
			self.insert(k.clone(), *p);
		}
	}

	fn remove(&mut self, key: &Key) -> Option<Payload> {
		self.keys.remove(key)
	}

	fn split_keys(self) -> Result<SplitKeys<Self>> {
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
		let (median_key, median_payload) = if let Some((k, v)) = s.next() {
			(k.clone(), *v)
		} else {
			fail!("BKeys/TrieKeys::split_keys")
		};
		let mut right = Trie::default();
		for (key, val) in s {
			right.insert(key.clone(), *val);
		}
		Ok(SplitKeys {
			left: Self::from(left),
			right: Self::from(right),
			median_idx,
			median_key,
			median_payload,
		})
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

	fn read_from(c: &mut Cursor<Vec<u8>>) -> Result<Self> {
		let compressed: Vec<u8> = bincode::deserialize_from(c)?;
		let mut uncompressed: Vec<u8> = Vec::new();
		{
			let mut rdr = snap::read::FrameDecoder::new(compressed.as_slice());
			io::copy(&mut rdr, &mut uncompressed)?;
		}
		let keys: Trie<Vec<u8>, u64> = bincode::deserialize_from(uncompressed.as_slice())?;
		Ok(Self {
			keys,
		})
	}

	fn write_to(&self, c: &mut Cursor<Vec<u8>>) -> Result<()> {
		let mut uncompressed: Vec<u8> = Vec::new();
		bincode::serialize_into(&mut uncompressed, &self.keys)?;
		let mut compressed: Vec<u8> = Vec::new();
		{
			let mut wtr = snap::write::FrameEncoder::new(&mut compressed);
			io::copy(&mut uncompressed.as_slice(), &mut wtr)?;
		}
		bincode::serialize_into(c, &compressed)?;
		Ok(())
	}
}

impl From<Trie<Key, Payload>> for TrieKeys {
	fn from(keys: Trie<Key, Payload>) -> Self {
		Self {
			keys,
		}
	}
}

struct KeysIterator<'a> {
	prefix: &'a Key,
	node_queue: VecDeque<SubTrie<'a, Key, Payload>>,
	current_node: Option<SubTrie<'a, Key, Payload>>,
}

impl<'a> KeysIterator<'a> {
	fn new(prefix: &'a Key, keys: &'a Trie<Key, Payload>) -> Self {
		let start_node = keys.get_raw_descendant(prefix);
		Self {
			prefix,
			node_queue: VecDeque::new(),
			current_node: start_node,
		}
	}

	fn next(&mut self) -> Option<(&Key, Payload)> {
		loop {
			if let Some(node) = self.current_node.take() {
				for children in node.children() {
					self.node_queue.push_front(children);
				}
				if let Some(value) = node.value() {
					if let Some(node_key) = node.key() {
						if node_key.starts_with(self.prefix) {
							return Some((node_key, *value));
						}
					}
				}
			} else {
				self.current_node = self.node_queue.pop_front();
				self.current_node.as_ref()?;
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::{HashMap, HashSet, VecDeque};
	use std::io::Cursor;

	use crate::idx::trees::bkeys::{BKeys, FstKeys, TrieKeys};
	use crate::idx::trees::btree::Payload;
	use crate::kvs::Key;

	fn test_keys_serde<BK: BKeys>(expected_size: usize) {
		let key: Key = "a".as_bytes().into();
		let mut keys = BK::with_key_val(key.clone(), 130).unwrap();
		keys.compile();
		// Serialize
		let mut cur: Cursor<Vec<u8>> = Cursor::new(Vec::new());
		keys.write_to(&mut cur).unwrap();
		let buf = cur.into_inner();
		assert_eq!(buf.len(), expected_size);
		// Deserialize
		let mut cur = Cursor::new(buf);
		let keys = BK::read_from(&mut cur).unwrap();
		assert_eq!(keys.get(&key), Some(130));
	}

	#[test]
	fn test_fst_keys_serde() {
		test_keys_serde::<FstKeys>(48);
	}

	#[test]
	fn test_trie_keys_serde() {
		test_keys_serde::<TrieKeys>(44);
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
			assert_eq!(keys.len() as usize, term_set.len());
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

	fn check_keys(r: VecDeque<(Key, Payload)>, e: Vec<(Key, Payload)>) {
		let mut map = HashMap::new();
		for (k, p) in r {
			map.insert(k, p);
		}
		assert_eq!(map.len(), e.len());
		for (k, p) in e {
			assert_eq!(map.get(&k), Some(&p));
		}
	}

	#[tokio::test]
	async fn test_tries_keys_collect_with_prefix() {
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
			let r = keys.collect_with_prefix(&"appli".into()).unwrap();
			check_keys(
				r,
				vec![("applicant".into(), 2), ("application".into(), 3), ("applicative".into(), 4)],
			);
		}

		{
			let r = keys.collect_with_prefix(&"the".into()).unwrap();
			check_keys(
				r,
				vec![
					("the".into(), 7),
					("their".into(), 8),
					("theirs".into(), 9),
					("there".into(), 10),
					("these".into(), 11),
					("theses".into(), 12),
				],
			);
		}

		{
			let r = keys.collect_with_prefix(&"blue".into()).unwrap();
			check_keys(r, vec![("blueberry".into(), 6)]);
		}

		{
			let r = keys.collect_with_prefix(&"apple".into()).unwrap();
			check_keys(r, vec![("apple".into(), 1)]);
		}

		{
			let r = keys.collect_with_prefix(&"zz".into()).unwrap();
			check_keys(r, vec![]);
		}
	}

	fn test_keys_split<BK: BKeys>(mut keys: BK) {
		keys.insert("a".into(), 1);
		keys.insert("b".into(), 2);
		keys.insert("c".into(), 3);
		keys.insert("d".into(), 4);
		keys.insert("e".into(), 5);
		keys.compile();
		let r = keys.split_keys().unwrap();
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
