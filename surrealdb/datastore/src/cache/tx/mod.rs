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
	cache: Base,
	/// Whether [`Self::speculative`] answers in place of [`Self::cache`].
	speculating: std::sync::atomic::AtomicBool,
	/// The entries read while the transaction speculates
	/// ([`crate::speculation`]): they describe the speculation's writes, so
	/// they are kept apart and dropped with it.
	speculative: std::sync::Mutex<Option<Layer>>,
	/// How many writes the speculation has taken, counted by its transactor.
	speculative_writes: std::sync::Mutex<Option<std::sync::Arc<std::sync::atomic::AtomicU64>>>,
	/// The speculated-on transaction's count of changes to what it reads, with
	/// the reading the speculation's entries were read over. A change to the
	/// base under a live speculation can make what it cached untrue, so the
	/// speculation then caches apart from what it read before.
	speculative_base: std::sync::Mutex<Option<(std::sync::Arc<std::sync::atomic::AtomicU64>, u64)>>,
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
		Self {
			cache: Base::new(size),
			speculating: std::sync::atomic::AtomicBool::new(false),
			speculative: std::sync::Mutex::new(None),
			speculative_writes: std::sync::Mutex::new(None),
			speculative_base: std::sync::Mutex::new(None),
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
			// The entry is in whichever cache answered when it was inserted;
			// removing it from both is safe, as a removal only forces a read.
			let lookup = key.as_lookup();
			self.cache.remove(&lookup);
			self.remove(&lookup);
		}
	}

	/// Answer from `kept` — entries read over the same speculative state
	/// earlier, shared with whatever else reads that state — or an empty cache
	/// of its own until [`Self::end_speculation`]. `writes` counts the
	/// speculation's writes: once it has taken one, what it caches describes
	/// the state the write leaves rather than the one `kept` is kept for, so
	/// it leaves `kept` for a cache of its own. `base` counts the changes
	/// to what the speculated-on transaction reads: once it moves, the
	/// speculation likewise leaves what it cached for a cache of its own.
	pub(crate) fn begin_speculation(
		&self,
		kept: Option<std::sync::Arc<Cache>>,
		writes: std::sync::Arc<std::sync::atomic::AtomicU64>,
		base: std::sync::Arc<std::sync::atomic::AtomicU64>,
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
		let seen = base.load(std::sync::atomic::Ordering::Acquire);
		*self.speculative_base.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) =
			Some((base, seen));
		self.speculating.store(true, std::sync::atomic::Ordering::Release);
	}

	/// Answer from the transaction's own entries again, handing back the
	/// entries read while speculating.
	pub(crate) fn end_speculation(&self) -> Option<std::sync::Arc<Cache>> {
		self.speculating.store(false, std::sync::atomic::Ordering::Release);
		self.speculative_base.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).take();
		self.speculative
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner())
			.take()
			.map(|layer| layer.cache)
	}

	/// Run `f` on the entries that answer now.
	fn with<R>(&self, f: impl FnOnce(Answering<'_>) -> R) -> R {
		if self.speculating.load(std::sync::atomic::Ordering::Acquire) {
			let wrote = self
				.speculative_writes
				.lock()
				.unwrap_or_else(|poisoned| poisoned.into_inner())
				.as_ref()
				.is_some_and(|writes| writes.load(std::sync::atomic::Ordering::Acquire) > 0);
			let base_changed = {
				let mut base =
					self.speculative_base.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
				match base.as_mut() {
					Some((count, seen)) => {
						let now = count.load(std::sync::atomic::Ordering::Acquire);
						let changed = now != *seen;
						*seen = now;
						changed
					}
					None => false,
				}
			};
			let mut speculative =
				self.speculative.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
			if let Some(layer) = speculative.as_mut() {
				if base_changed || (wrote && layer.shared) {
					*layer = Layer {
						cache: std::sync::Arc::new(speculative_cache()),
						shared: false,
					};
				}
				return f(Answering::Speculation(&layer.cache));
			}
		}
		f(Answering::Own(&self.cache))
	}

	/// Fetch an item from the datastore cache
	pub fn get(&self, lookup: &Lookup) -> Option<Entry> {
		self.with(|answering| match answering {
			Answering::Speculation(cache) => cache.get(lookup),
			Answering::Own(own) => own.get(lookup),
		})
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
		self.with(|answering| match answering {
			Answering::Speculation(cache) => cache.insert(key, entry),
			Answering::Own(own) => own.insert(key, entry),
		});
	}

	/// Remove an item from the datastore cache
	pub fn remove(&self, lookup: &Lookup<'_>) {
		self.with(|answering| match answering {
			Answering::Speculation(cache) => {
				cache.remove(lookup);
			}
			Answering::Own(own) => own.remove(lookup),
		});
	}

	/// Clear all items from the datastore cache
	pub fn clear(&self) {
		self.with(|answering| match answering {
			Answering::Speculation(cache) => cache.clear(),
			Answering::Own(own) => own.clear(),
		});
	}
}

/// The entries that answer a transaction's cache now.
enum Answering<'a> {
	/// A speculation's ([`TransactionCache::begin_speculation`]).
	Speculation(&'a Cache),
	/// The transaction's own.
	Own(&'a Base),
}

/// How many entries [`Base`] holds in a list before it holds them in a [`Cache`].
const FEW: usize = 16;

/// A transaction's own entries. Most transactions cache a handful, which are
/// held in a list searched in order, cheaper to fill, search and drop than a
/// [`Cache`]; past [`FEW`] of them they move to a [`Cache`]. The list is used
/// only where the cache's capacity is at least [`FEW`], so it holds no entry
/// the cache would have evicted.
pub(crate) struct Base {
	state: std::sync::Mutex<BaseState>,
	/// The capacity of the [`Cache`] the entries move to.
	size: usize,
}

enum BaseState {
	Few(Vec<(key::Key, Entry)>),
	Many(Cache),
}

impl Base {
	fn new(size: usize) -> Self {
		let state = if size >= FEW {
			BaseState::Few(Vec::new())
		} else {
			BaseState::Many(base_cache(size))
		};
		Self {
			state: std::sync::Mutex::new(state),
			size,
		}
	}

	fn state(&self) -> std::sync::MutexGuard<'_, BaseState> {
		self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
	}

	fn get(&self, lookup: &Lookup<'_>) -> Option<Entry> {
		use quick_cache::Equivalent;
		match &*self.state() {
			BaseState::Few(entries) => entries
				.iter()
				.find(|(key, _)| lookup.equivalent(key))
				.map(|(_, entry)| entry.clone()),
			BaseState::Many(cache) => cache.get(lookup),
		}
	}

	fn insert(&self, key: key::Key, entry: Entry) {
		let mut state = self.state();
		match &mut *state {
			BaseState::Few(entries) => {
				if let Some(held) = entries.iter_mut().find(|(held, _)| *held == key) {
					held.1 = entry;
				} else if entries.len() < FEW {
					entries.push((key, entry));
				} else {
					let cache = base_cache(self.size);
					for (key, entry) in entries.drain(..) {
						cache.insert(key, entry);
					}
					cache.insert(key, entry);
					*state = BaseState::Many(cache);
				}
			}
			BaseState::Many(cache) => cache.insert(key, entry),
		}
	}

	fn remove(&self, lookup: &Lookup<'_>) {
		use quick_cache::Equivalent;
		match &mut *self.state() {
			BaseState::Few(entries) => entries.retain(|(key, _)| !lookup.equivalent(key)),
			BaseState::Many(cache) => {
				cache.remove(lookup);
			}
		}
	}

	fn clear(&self) {
		match &mut *self.state() {
			BaseState::Few(entries) => entries.clear(),
			BaseState::Many(cache) => cache.clear(),
		}
	}
}

/// The cache a transaction's own entries are held in past [`FEW`] of them,
/// with `size` its capacity.
fn base_cache(size: usize) -> Cache {
	let options = OptionsBuilder::new()
		.estimated_items_capacity(size)
		.weight_capacity(size as u64)
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

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use surrealdb_catalog::NamespaceId;

	use super::{Entry, FEW, Lookup, TransactionCache};

	fn entry(n: u32) -> Entry {
		Entry::Any(Arc::new(n))
	}

	fn read(cache: &TransactionCache, ns: u32) -> Option<u32> {
		let Entry::Any(any) = cache.get(&Lookup::Nus(NamespaceId(ns)))? else {
			return None;
		};
		any.downcast_ref::<u32>().copied()
	}

	/// The entries answer alike whether the cache holds few of them or has
	/// moved them past [`FEW`]: an insert replaces, a removal and a clear
	/// forget.
	#[test]
	fn entries_answer_alike_before_and_after_they_move() {
		let cache = TransactionCache::new(512);
		let count = 3 * FEW as u32;
		for n in 0..count {
			cache.insert(Lookup::Nus(NamespaceId(n)), entry(n));
			// Replaced, not added again.
			cache.insert(Lookup::Nus(NamespaceId(n)), entry(n + 1000));
			for held in 0..=n {
				assert_eq!(read(&cache, held), Some(held + 1000), "{held} of {n}");
			}
			assert_eq!(read(&cache, n + 1), None);
		}
		cache.remove(&Lookup::Nus(NamespaceId(1)));
		assert_eq!(read(&cache, 1), None);
		assert_eq!(read(&cache, 2), Some(1002));
		cache.clear();
		assert_eq!(read(&cache, 2), None);
	}

	/// A rolled-back save point forgets what was inserted inside it, across
	/// the move past [`FEW`].
	#[test]
	fn a_rolled_back_save_point_forgets_across_the_move() {
		let cache = TransactionCache::new(512);
		cache.insert(Lookup::Nus(NamespaceId(0)), entry(0));
		cache.push_save_point();
		for n in 1..=FEW as u32 {
			cache.insert(Lookup::Nus(NamespaceId(n)), entry(n));
		}
		cache.rollback_save_point();
		assert_eq!(read(&cache, 0), Some(0));
		for n in 1..=FEW as u32 {
			assert_eq!(read(&cache, n), None, "{n}");
		}
	}

	/// A cache too small to hold [`FEW`] entries keeps its capacity.
	#[test]
	fn a_small_cache_keeps_its_capacity() {
		let cache = TransactionCache::new(1);
		for n in 0..4 {
			cache.insert(Lookup::Nus(NamespaceId(n)), entry(n));
		}
		// `Any` entries weigh nothing and are never evicted.
		for n in 0..4 {
			assert_eq!(read(&cache, n), Some(n));
		}
	}
}
