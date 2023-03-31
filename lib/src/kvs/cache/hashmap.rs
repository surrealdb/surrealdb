use crate::kvs::cache::{Entry, SyncCache};
use crate::kvs::Key;
use std::collections::HashMap;

#[derive(Default)]
pub struct HashTableCache(pub HashMap<Key, Entry>);

impl SyncCache for HashTableCache {
	// Check if key exists
	fn exi(&mut self, key: &Key) -> bool {
		self.0.contains_key(key)
	}
	// Set a key in the cache
	fn set(&mut self, key: Key, val: Entry) {
		self.0.insert(key, val);
	}
	// Get a key from the cache
	fn get(&mut self, key: &Key) -> Option<Entry> {
		self.0.get(key).cloned()
	}
	// Delete a key from the cache
	fn del(&mut self, key: &Key) -> Option<Entry> {
		self.0.remove(key)
	}
}
