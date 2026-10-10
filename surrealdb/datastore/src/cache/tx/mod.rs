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
	/// One frame per open save point, innermost last, each holding the keys
	/// inserted while it was the innermost. An entry inserted inside a save
	/// point may describe a write the save point's rollback undoes, so
	/// [`Self::rollback_save_point`] evicts every key in the frame it closes.
	save_points: std::sync::Mutex<Vec<Vec<key::Key>>>,
	/// Whether any save point is open, so an insert outside one takes no lock.
	journaling: std::sync::atomic::AtomicBool,
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
			save_points: std::sync::Mutex::new(Vec::new()),
			journaling: std::sync::atomic::AtomicBool::new(false),
		}
	}

	/// Open a save point: the entries inserted from here until it is released
	/// or rolled back are recorded in a frame of their own.
	pub(crate) fn push_save_point(&self) {
		let mut frames = self.save_points.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		frames.push(Vec::new());
		self.journaling.store(true, std::sync::atomic::Ordering::Release);
	}

	/// Release the innermost save point. Its writes now belong to the
	/// enclosing save point, so its entries join the enclosing frame, where an
	/// outer rollback still reaches them.
	pub(crate) fn release_save_point(&self) {
		let mut frames = self.save_points.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		if let Some(frame) = frames.pop()
			&& let Some(outer) = frames.last_mut()
		{
			outer.extend(frame);
		}
		self.journaling.store(!frames.is_empty(), std::sync::atomic::Ordering::Release);
	}

	/// Roll back the innermost save point, evicting every entry inserted while
	/// it was open. An evicted entry is read again from storage, which the
	/// rollback has already returned to its state at the save point.
	pub(crate) fn rollback_save_point(&self) {
		let frame = {
			let mut frames =
				self.save_points.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
			let frame = frames.pop();
			self.journaling.store(!frames.is_empty(), std::sync::atomic::Ordering::Release);
			frame
		};
		for key in frame.into_iter().flatten() {
			self.cache.remove(&key);
		}
	}

	/// Fetch an item from the datastore cache
	pub fn get(&self, lookup: &Lookup) -> Option<Entry> {
		self.cache.get(lookup)
	}

	/// Insert an item into the datastore cache
	pub fn insert(&self, lookup: Lookup, entry: Entry) {
		let key = key::Key::from(lookup);
		if self.journaling.load(std::sync::atomic::Ordering::Acquire) {
			let mut frames =
				self.save_points.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
			if let Some(frame) = frames.last_mut() {
				frame.push(key.clone());
			}
		}
		self.cache.insert(key, entry);
	}

	/// Remove an item from the datastore cache
	pub fn remove(&self, lookup: &Lookup<'_>) {
		self.cache.remove(lookup);
	}

	/// Clear all items from the datastore cache
	pub fn clear(&self) {
		self.cache.clear();
	}
}
