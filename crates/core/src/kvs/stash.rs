use crate::idg::u32::U32;
use crate::kvs::kv::Key;
use std::collections::HashMap;

#[derive(Default)]
pub(super) struct Stash(pub HashMap<Key, U32>);

impl Stash {
	/// Set a key in the cache
	pub fn set(&mut self, key: Key, val: U32) {
		self.0.insert(key, val);
	}
	/// Get a key from the cache
	pub fn get(&mut self, key: &Key) -> Option<U32> {
		self.0.get(key).cloned()
	}
}
