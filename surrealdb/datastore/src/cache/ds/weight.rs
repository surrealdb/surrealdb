use quick_cache::Weighter;

use super::entry::Entry;
use super::key::Key;

#[derive(Clone)]
pub struct Weight;

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
			// All other entries, the per-table catalog snapshot included, have
			// the same weight, and can be evicted when necessary.
			_ => 1,
		}
	}
}
