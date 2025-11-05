use quick_cache::Weighter;

use super::entry::Entry;
use super::key::Key;

#[derive(Clone)]
pub(crate) struct Weight;

impl Weighter<Key, Entry> for Weight {
	fn weight(&self, _key: &Key, val: &Entry) -> u64 {
		match val {
			// Value entries all have the same weight,
			// and can be evicted whenever necessary.
			// We could improve this, by calculating
			// the precise weight of a Value (when
			// deserialising), and using this size to
			// determine the actual cache weight.
			Entry::Val(_) => 1,
			// We don't want to evict other entries
			// so we set the weight to 0 which will
			// prevent entries being evicted, unless
			// specifically removed from the cache.
			_ => 0,
		}
	}
}
