use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicUsize};

use ahash::{HashMap, HashMapExt};
use futures::future::join_all;
use tokio::sync::Mutex;

pub(super) type CacheKey = u64;

pub(super) struct ConcurrentLru<V>
where
	V: Clone,
{
	/// Each shard is a LRU cache
	shards: Vec<Mutex<LruShard<V>>>,
	/// Keep track of sizes for each shard
	lengths: Vec<AtomicUsize>,
	/// If lengths == capacity, then full is true
	full: AtomicBool,
	/// The number of shards
	shards_count: usize,
	/// The maximum capacity
	capacity: usize,
}

impl<V> ConcurrentLru<V>
where
	V: Clone,
{
	pub(super) fn with_capacity(capacity: usize) -> Self {
		// slightly more than the number of CPU cores
		let shards_count = (num_cpus::get() * 4 / 3).min(capacity);
		let mut shards = Vec::with_capacity(shards_count);
		let mut lengths = Vec::with_capacity(shards_count);
		for _ in 0..shards_count {
			shards.push(Mutex::new(LruShard::new()));
			lengths.push(AtomicUsize::new(0));
		}
		Self {
			shards_count,
			shards,
			lengths,
			full: AtomicBool::new(false),
			capacity,
		}
	}
	pub(super) async fn get<K: Into<CacheKey>>(&self, key: K) -> Option<V> {
		let key = key.into();
		// Locate the shard
		let n = key as usize % self.shards_count;
		// Get and promote the key
		self.shards[n].lock().await.get_and_promote(key)
	}

	pub(super) async fn insert<K: Into<CacheKey>>(&self, key: K, val: V) {
		let key = key.into();
		// Locate the shard
		let shard = key as usize % self.shards_count;
		// Insert the key/object in the shard and get the new length
		let new_length = self.shards[shard].lock().await.insert(key, val, self.full.load(Relaxed));
		// Update lengths
		self.check_length(new_length, shard);
	}

	pub(super) async fn remove<K: Into<CacheKey>>(&self, key: K) {
		let key = key.into();
		// Locate the shard
		let shard = key as usize % self.shards_count;
		// Remove the key
		let new_length = self.shards[shard].lock().await.remove(key);
		// Update lengths
		self.check_length(new_length, shard);
	}

	fn check_length(&self, new_length: usize, shard: usize) {
		// Set the length for this shard
		self.lengths[shard].store(new_length, Relaxed);
		// Compute the total length
		let total_length: usize = self.lengths.iter().map(|l| l.load(Relaxed)).sum();
		// Check if the cache is full
		self.full.store(total_length == self.capacity, Relaxed);
	}

	#[cfg(test)]
	pub(super) fn len(&self) -> usize {
		self.lengths.iter().map(|l| l.load(Relaxed)).sum()
	}

	pub(super) async fn duplicate<F>(&self, filter: F) -> Self
	where
		F: Fn(&CacheKey) -> bool + Copy,
	{
		// We cant the shards to be copied concurrently.
		// So we create one future per shard.
		let futures: Vec<_> = self
			.shards
			.iter()
			.map(|s| async {
				let shard = s.lock().await.duplicate(filter);
				(shard.map.len(), Mutex::new(shard))
			})
			.collect();
		let mut shards = Vec::with_capacity(self.shards_count);
		let mut lengths = Vec::with_capacity(self.shards_count);
		let mut total_lengths = 0;
		for (length, shard) in join_all(futures).await {
			shards.push(shard);
			total_lengths += length;
			lengths.push(length.into());
		}
		Self {
			shards,
			lengths,
			full: AtomicBool::new(total_lengths >= self.capacity),
			shards_count: self.shards_count,
			capacity: self.capacity,
		}
	}
}

struct LruShard<T>
where
	T: Clone,
{
	map: HashMap<CacheKey, usize>,
	vec: Vec<Option<(CacheKey, T)>>,
}

impl<T> LruShard<T>
where
	T: Clone,
{
	fn new() -> Self {
		Self {
			map: HashMap::default(),
			vec: Vec::new(),
		}
	}

	fn get_and_promote(&mut self, key: CacheKey) -> Option<T> {
		if let Some(pos) = self.map.get(&key).copied() {
			let val = self.vec[pos].clone();
			if pos > 0 {
				self.promote(key, pos);
			}
			val.map(|(_, v)| v.clone())
		} else {
			None
		}
	}

	// The promotion implements a very low-cost strategy.
	// Promotion is done by flipping the entry with the entry just before.
	// Each time an entry is hit, its weight increase.
	fn promote(&mut self, key: CacheKey, pos: usize) {
		// Promotion is flipping the current entry with the entry before
		let new_pos = pos - 1;
		let flip_key = self.vec[new_pos].as_ref().map(|(k, _)| k).copied();
		self.vec.swap(pos, new_pos);
		self.map.insert(key, new_pos);
		if let Some(flip_key) = flip_key {
			self.map.insert(flip_key, pos);
		} else if pos == self.vec.len() - 1 {
			self.vec.remove(pos);
		}
	}

	fn insert(&mut self, key: CacheKey, val: T, replace: bool) -> usize {
		if let Some(pos) = self.map.get(&key).copied() {
			// If the entry is already there, just update it
			self.vec[pos] = Some((key, val));
		} else {
			// If we reached the capacity
			if replace {
				// Find the last entry...
				while !self.vec.is_empty() {
					if let Some(Some((k, _v))) = self.vec.pop() {
						// ... and remove it
						self.map.remove(&k);
						break;
					}
				}
			}
			// Now we can insert the new entry
			let pos = self.vec.len();
			self.vec.push(Some((key, val)));
			// If it is the head
			if pos == 0 {
				// ...we just insert it
				self.map.insert(key, pos);
			} else {
				// or we promote it
				self.promote(key, pos);
			}
		}
		self.map.len()
	}

	fn remove(&mut self, key: CacheKey) -> usize {
		if let Some(pos) = self.map.remove(&key) {
			if pos == self.vec.len() - 1 {
				// If it is the last element, we can just remove it from the vec
				self.vec.pop();
			} else {
				// Otherwise we set a placeholder
				self.vec[pos] = None;
			}
		}
		self.map.len()
	}

	/// Make a copy of this cache containing every entry for which the specified
	/// filter returns true.
	fn duplicate<F>(&self, filter: F) -> Self
	where
		F: Fn(&CacheKey) -> bool,
	{
		let mut map = HashMap::with_capacity(self.map.len());
		let mut vec = Vec::with_capacity(self.vec.len());
		self.map.iter().filter(|&(k, _pos)| filter(k)).for_each(|(k, pos)| {
			let new_pos = vec.len();
			map.insert(*k, new_pos);
			vec.push(self.vec[*pos].clone());
		});
		Self {
			map,
			vec,
		}
	}
}
#[cfg(test)]
mod tests {
	use futures::future::join_all;
	use test_log::test;

	use super::ConcurrentLru;

	#[test(tokio::test)]
	async fn test_minimal_tree_lru() {
		let lru = ConcurrentLru::with_capacity(1);
		assert_eq!(lru.len(), 0);
		//
		lru.insert(1u64, 'a').await;
		assert_eq!(lru.len(), 1);
		assert_eq!(lru.get(1u64).await, Some('a'));
		//
		lru.insert(2u64, 'b').await;
		assert_eq!(lru.len(), 1);
		assert_eq!(lru.get(1u64).await, None);
		assert_eq!(lru.get(2u64).await, Some('b'));
		//
		lru.insert(2u64, 'c').await;
		assert_eq!(lru.len(), 1);
		assert_eq!(lru.get(2u64).await, Some('c'));
		//
		lru.remove(1u64).await;
		assert_eq!(lru.len(), 1);
		assert_eq!(lru.get(2u64).await, Some('c'));
		//
		lru.remove(2u64).await;
		assert_eq!(lru.len(), 0);
		assert_eq!(lru.get(1u64).await, None);
		assert_eq!(lru.get(2u64).await, None);
	}

	#[test(tokio::test)]
	async fn test_tree_lru() {
		let lru = ConcurrentLru::with_capacity(4);
		//
		lru.insert(1u64, 'a').await;
		lru.insert(2u64, 'b').await;
		lru.insert(3u64, 'c').await;
		lru.insert(4u64, 'd').await;
		assert_eq!(lru.len(), 4);
		assert_eq!(lru.get(1u64).await, Some('a'));
		assert_eq!(lru.get(2u64).await, Some('b'));
		assert_eq!(lru.get(3u64).await, Some('c'));
		assert_eq!(lru.get(4u64).await, Some('d'));
		//
		lru.insert(5u64, 'e').await;
		assert_eq!(lru.len(), 4);
		assert_eq!(lru.get(1u64).await, None);
		assert_eq!(lru.get(2u64).await, Some('b'));
		assert_eq!(lru.get(3u64).await, Some('c'));
		assert_eq!(lru.get(4u64).await, Some('d'));
		assert_eq!(lru.get(5u64).await, Some('e'));
		//
		let lru = lru.duplicate(|k| *k != 3).await;
		assert_eq!(lru.len(), 3);
		assert_eq!(lru.get(1u64).await, None);
		assert_eq!(lru.get(2u64).await, Some('b'));
		assert_eq!(lru.get(3u64).await, None);
		assert_eq!(lru.get(4u64).await, Some('d'));
		assert_eq!(lru.get(5u64).await, Some('e'));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn concurrent_lru_test() {
		let num_threads = 4;
		let lru = ConcurrentLru::with_capacity(100);

		let futures: Vec<_> = (0..num_threads)
			.map(|_| async {
				lru.insert(10u64, 'a').await;
				lru.get(10u64).await;
				lru.insert(20u64, 'b').await;
				lru.remove(10u64).await;
			})
			.collect();

		join_all(futures).await;

		assert!(lru.get(10u64).await.is_none());
		assert!(lru.get(20u64).await.is_some());
	}
}
