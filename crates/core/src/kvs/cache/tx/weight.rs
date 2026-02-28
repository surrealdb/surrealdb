use super::entry::Entry;
use super::key::Key;
use quick_cache::Weighter;

#[derive(Clone)]
pub(crate) struct Weight;

impl Weighter<Key, Entry> for Weight {
	fn weight(&self, _key: &Key, val: &Entry) -> u32 {
		match val {
			// Value entries all have the same weight,
			// and can be evicted whenever necessary.
			// We could improve this, by calculating
			// the precise weight of a Value (when
			// deserialising), and using this size to
			// determine the actual cache weight.
			Entry::Val(_) => 2,
			// We prefer to not evict other entries
			// so we set the weight to 1 which will
			// evict other entries before these.
			_ => 1,
		}
	}
}
