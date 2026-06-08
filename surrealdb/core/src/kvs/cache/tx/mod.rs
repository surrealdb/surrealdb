mod entry;
mod key;
mod lookup;
mod weight;

pub(crate) use entry::Entry;
pub(crate) use lookup::Lookup;
use quick_cache::sync::DefaultLifecycle;
use quick_cache::{DefaultHashBuilder, OptionsBuilder};

/// Per-transaction cache backed by `quick_cache` (see [`TransactionCache::new`]
/// for shard settings).
///
/// # Values
/// Cache values are cloned when fetched. Value types should be wrapped in an
/// `Arc<_>` to avoid expensive clone operations when retrieving values from the
/// cache. If interior mutability is required, `Arc<Mutex<_>>` or `Arc<RwLock<_>>`
/// can be used.
///
/// # Thread safety
/// The cache instance can be wrapped with an `Arc` and safely shared between
/// threads. All methods are accessible via non-mut references, so no further
/// synchronisation with mutexes is required for the cache itself.
pub(crate) type Cache = quick_cache::sync::Cache<key::Key, Entry, weight::Weight>;

/// Transaction-scoped cache; see [`Cache`] for behaviour notes.
pub struct TransactionCache {
	/// Store the cache entries
	cache: Cache,
}

impl TransactionCache {
	/// Creates a new empty cache for a transaction.
	///
	/// The cache is per-transaction and not concurrently accessed across
	/// threads, so `shards = 1` is used. The default `available_parallelism() *
	/// 4` would allocate hundreds of sharded `CacheShard` structs per
	/// transaction on large boxes, all of which then get dropped at commit or
	/// cancel.
	pub(in crate::kvs) fn new(size: usize) -> Self {
		let options = OptionsBuilder::new()
			.estimated_items_capacity(size)
			.weight_capacity(size as u64)
			.shards(1)
			.build()
			.expect("valid transaction cache options");
		let cache = Cache::with_options(
			options,
			weight::Weight,
			DefaultHashBuilder::default(),
			DefaultLifecycle::default(),
		);
		Self {
			cache,
		}
	}

	/// Fetch an item from the datastore cache
	pub(crate) fn get(&self, lookup: &Lookup) -> Option<Entry> {
		self.cache.get(lookup)
	}

	/// Insert an item into the datastore cache
	pub(crate) fn insert(&self, lookup: Lookup, entry: Entry) {
		self.cache.insert(lookup.into(), entry);
	}

	/// Remove an item from the datastore cache
	pub(crate) fn remove(&self, lookup: &Lookup<'_>) {
		self.cache.remove(lookup);
	}

	/// Clear all items from the datastore cache
	pub(crate) fn clear(&self) {
		self.cache.clear();
	}
}
