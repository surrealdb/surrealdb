use super::entry::Entry;
use super::key::Key;
use quick_cache::Weighter;

#[derive(Clone)]
pub(crate) struct Weight;

impl Weighter<Key, Entry> for Weight {
	fn weight(&self, _key: &Key, _val: &Entry) -> u32 {
		// For the moment all entries have the
		// same weight, and can be evicted when
		// necessary. In the future we will
		// compute the actual size of the value
		// in memory and use that for the weight.
		1
	}
}
