use std::collections::HashMap;

use crate::idg::u32::U32;

#[derive(Default)]
pub(super) struct Stash(pub HashMap<Vec<u8>, U32>);

impl Stash {
	/// Set a key in the cache
	pub fn set(&mut self, key: Vec<u8>, val: U32) {
		self.0.insert(key, val);
	}
	/// Get a key from the cache
	pub fn get(&mut self, key: &[u8]) -> Option<U32> {
		self.0.get(key).cloned()
	}
}
