use quick_cache::Weighter;

use super::entry::Entry;
use super::key::Key;

#[derive(Clone)]
pub(crate) struct Weight;

impl Weighter<Key, Entry> for Weight {
	fn weight(&self, _key: &Key, val: &Entry) -> u64 {
		match val {
			// JWKS documents are keyed by the URL of admin-configured access
			// methods, so the working set is bounded by configuration rather
			// than by request volume. Pin them (weight 0) so cache pressure
			// from schema or live-query traffic cannot evict them and bypass
			// the JWKS refresh cooldown.
			#[cfg(feature = "jwks")]
			Entry::Jwk(_) => 0,
			// For the moment all other entries have the same weight, and can
			// be evicted when necessary. In the future we will compute the
			// actual size of the value in memory and use that for the weight.
			_ => 1,
		}
	}
}
