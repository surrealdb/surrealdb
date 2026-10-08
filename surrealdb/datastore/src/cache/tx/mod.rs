mod entry;
mod key;
mod lookup;
mod weight;

pub use entry::Entry;
pub use lookup::Lookup;
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
pub type Cache = quick_cache::sync::Cache<key::Key, Entry, weight::Weight>;

/// Transaction-scoped cache; see [`Cache`] for behaviour notes.
pub struct TransactionCache {
	/// Store the cache entries
	cache: Cache,
	/// Whether [`Self::speculative`] answers in place of [`Self::cache`].
	speculating: std::sync::atomic::AtomicBool,
	/// The entries read while the transaction speculates
	/// ([`crate::speculation`]): they describe the speculation's writes, so
	/// they are kept apart and dropped with it.
	speculative: std::sync::Mutex<Option<Layer>>,
	/// How many writes the speculation has taken, counted by its transactor.
	speculative_writes: std::sync::Mutex<Option<std::sync::Arc<std::sync::atomic::AtomicU64>>>,
}

impl TransactionCache {
	/// Creates a new empty cache for a transaction.
	///
	/// The cache is per-transaction and not concurrently accessed across
	/// threads, so `shards = 1` is used. The default `available_parallelism() *
	/// 4` would allocate hundreds of sharded `CacheShard` structs per
	/// transaction on large boxes, all of which then get dropped at commit or
	/// cancel.
	pub fn new(size: usize) -> Self {
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
			speculating: std::sync::atomic::AtomicBool::new(false),
			speculative: std::sync::Mutex::new(None),
			speculative_writes: std::sync::Mutex::new(None),
		}
	}

	/// Answer from `kept` — entries read over the same speculative state
	/// earlier, shared with whatever else reads that state — or an empty cache
	/// of its own until [`Self::end_speculation`]. `writes` counts the
	/// speculation's writes: once it has taken one, what it caches describes
	/// the state the write leaves rather than the one `kept` is kept for, so
	/// it leaves `kept` for a cache of its own.
	pub(crate) fn begin_speculation(
		&self,
		kept: Option<std::sync::Arc<Cache>>,
		writes: std::sync::Arc<std::sync::atomic::AtomicU64>,
	) {
		let layer = match kept {
			Some(cache) => Layer {
				cache,
				shared: true,
			},
			None => Layer {
				cache: std::sync::Arc::new(speculative_cache()),
				shared: false,
			},
		};
		*self.speculative.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(layer);
		*self.speculative_writes.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) =
			Some(writes);
		self.speculating.store(true, std::sync::atomic::Ordering::Release);
	}

	/// Answer from the transaction's own entries again, handing back the
	/// entries read while speculating.
	pub(crate) fn end_speculation(&self) -> Option<std::sync::Arc<Cache>> {
		self.speculating.store(false, std::sync::atomic::Ordering::Release);
		self.speculative
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner())
			.take()
			.map(|layer| layer.cache)
	}

	/// Run `f` on the cache that answers now.
	fn with<R>(&self, f: impl FnOnce(&Cache) -> R) -> R {
		if self.speculating.load(std::sync::atomic::Ordering::Acquire) {
			let wrote = self
				.speculative_writes
				.lock()
				.unwrap_or_else(|poisoned| poisoned.into_inner())
				.as_ref()
				.is_some_and(|writes| writes.load(std::sync::atomic::Ordering::Acquire) > 0);
			let mut speculative =
				self.speculative.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
			if let Some(layer) = speculative.as_mut() {
				if wrote && layer.shared {
					*layer = Layer {
						cache: std::sync::Arc::new(speculative_cache()),
						shared: false,
					};
				}
				return f(&layer.cache);
			}
		}
		f(&self.cache)
	}

	/// Fetch an item from the datastore cache
	pub fn get(&self, lookup: &Lookup) -> Option<Entry> {
		self.with(|cache| cache.get(lookup))
	}

	/// Insert an item into the datastore cache
	pub fn insert(&self, lookup: Lookup, entry: Entry) {
		self.with(|cache| cache.insert(lookup.into(), entry));
	}

	/// Remove an item from the datastore cache
	pub fn remove(&self, lookup: &Lookup<'_>) {
		self.with(|cache| {
			cache.remove(lookup);
		});
	}

	/// Clear all items from the datastore cache
	pub fn clear(&self) {
		self.with(|cache| cache.clear());
	}
}

/// The cache a speculation answers from.
pub(crate) struct Layer {
	cache: std::sync::Arc<Cache>,
	/// Whether the cache was kept from an earlier speculation, and so is
	/// shared with the speculations from the same state after this one.
	shared: bool,
}

/// An empty cache for a speculation's reads.
fn speculative_cache() -> Cache {
	let options = OptionsBuilder::new()
		.estimated_items_capacity(256)
		.weight_capacity(256)
		.shards(1)
		.build()
		.expect("valid transaction cache options");
	Cache::with_options(
		options,
		weight::Weight,
		DefaultHashBuilder::default(),
		DefaultLifecycle::default(),
	)
}
