use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;
use quick_cache::{Equivalent, Weighter};
use surrealism_runtime::controller::Runtime;
use tokio::sync::Mutex;

use crate::catalog::{DatabaseId, NamespaceId};

pub struct SurrealismCache {
	cache: quick_cache::sync::Cache<SurrealismCacheKey, SurrealismCacheValue, Weight>,
	// Tracks in-progress plugin compilations to prevent duplicate work
	loading: DashMap<SurrealismCacheKey, Arc<Mutex<()>>>,
}

impl SurrealismCache {
	pub fn new() -> Self {
		Self {
			cache: quick_cache::sync::Cache::with_weighter(
				*crate::cnf::SURREALISM_CACHE_SIZE,
				*crate::cnf::SURREALISM_CACHE_SIZE as u64,
				Weight,
			),
			loading: DashMap::new(),
		}
	}

	pub fn get(&self, lookup: &SurrealismCacheLookup) -> Option<SurrealismCacheValue> {
		self.cache.get(lookup)
	}

	pub fn remove(&self, lookup: &SurrealismCacheLookup) {
		self.cache.remove(lookup);
	}

	/// Get from cache or compute the value if not present.
	/// Only one task will compute for a given key; others will wait.
	pub async fn insert_if_not_exists<F, Fut>(
		&self,
		lookup: SurrealismCacheLookup<'_>,
		compute: F,
	) -> Result<Arc<Runtime>>
	where
		F: FnOnce(SurrealismCacheKey) -> Fut,
		Fut: Future<Output = Result<Arc<Runtime>>>,
	{
		// Fast path: already cached
		if let Some(value) = self.get(&lookup) {
			return Ok(value.runtime);
		}

		let cache_key: SurrealismCacheKey = lookup.into();
		let loading_lock = self.get_loading_lock(&cache_key);
		let _guard = loading_lock.lock().await;

		// Double-check after acquiring lock
		// Another task may have completed compilation while we waited
		if let Some(value) = self.get(&cache_key.as_lookup()) {
			self.remove_loading_lock(&cache_key);
			return Ok(value.runtime);
		}

		// We're the first task, compute the value
		// Clone the key to pass to compute closure
		let result = compute(cache_key.clone()).await;

		// Always clean up loading lock
		self.remove_loading_lock(&cache_key);

		// If successful, cache the result
		match result {
			Ok(runtime) => {
				self.cache.insert(
					cache_key,
					SurrealismCacheValue {
						runtime: runtime.clone(),
					},
				);
				Ok(runtime)
			}
			Err(e) => Err(e),
		}
	}

	/// Get or create a loading lock for a specific plugin.
	/// This ensures only one task compiles a given plugin at a time.
	fn get_loading_lock(&self, key: &SurrealismCacheKey) -> Arc<Mutex<()>> {
		self.loading.entry(key.clone()).or_insert_with(|| Arc::new(Mutex::new(()))).clone()
	}

	/// Remove a loading lock after compilation completes or fails.
	fn remove_loading_lock(&self, key: &SurrealismCacheKey) {
		self.loading.remove(key);
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub enum SurrealismCacheKey {
	// NS - DB - BUCKET - KEY
	File(NamespaceId, DatabaseId, String, String),
	// Organisation - Package - MAJOR - MINOR - PATCH
	Silo(String, String, u32, u32, u32),
}

impl SurrealismCacheKey {
	pub fn as_lookup(&self) -> SurrealismCacheLookup<'_> {
		match self {
			SurrealismCacheKey::File(ns, db, bucket, key) => {
				SurrealismCacheLookup::File(ns, db, bucket.as_str(), key.as_str())
			}
			SurrealismCacheKey::Silo(org, pkg, maj, min, pat) => {
				SurrealismCacheLookup::Silo(org.as_str(), pkg.as_str(), *maj, *min, *pat)
			}
		}
	}
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub enum SurrealismCacheLookup<'a> {
	// NS - DB - BUCKET - KEY
	File(&'a NamespaceId, &'a DatabaseId, &'a str, &'a str),
	// Organisation - Package - MAJOR - MINOR - PATCH
	Silo(&'a str, &'a str, u32, u32, u32),
}

impl<'a> From<SurrealismCacheLookup<'a>> for SurrealismCacheKey {
	fn from(lookup: SurrealismCacheLookup<'a>) -> Self {
		match lookup {
			SurrealismCacheLookup::File(ns, db, bucket, key) => {
				SurrealismCacheKey::File(*ns, *db, bucket.to_owned(), key.to_owned())
			}
			SurrealismCacheLookup::Silo(org, pkg, maj, min, pat) => {
				SurrealismCacheKey::Silo(org.to_string(), pkg.to_string(), maj, min, pat)
			}
		}
	}
}

impl Equivalent<SurrealismCacheKey> for SurrealismCacheLookup<'_> {
	#[rustfmt::skip]
	fn equivalent(&self, key: &SurrealismCacheKey) -> bool {
        match (self, key) {
            (Self::File(a1, b1, c1, d1), SurrealismCacheKey::File(a2, b2, c2, d2))
                => a1.0 == a2.0 && b1.0 == b2.0 && c1 == c2 && d1 == d2,
            (Self::Silo(a1, b1, c1, d1, e1), SurrealismCacheKey::Silo(a2, b2, c2, d2, e2))
                => a1 == a2 && b1 == b2 && c1 == c2 && d1 == d2 && e1 == e2,
            _ => false,
        }
    }
}

#[derive(Clone)]
pub struct SurrealismCacheValue {
	pub(crate) runtime: Arc<Runtime>,
}

#[derive(Clone)]
pub(crate) struct Weight;

impl Weighter<SurrealismCacheKey, SurrealismCacheValue> for Weight {
	fn weight(&self, _key: &SurrealismCacheKey, _val: &SurrealismCacheValue) -> u64 {
		// For the moment all entries have the
		// same weight, and can be evicted when
		// necessary. In the future we will
		// compute the actual size of the value
		// in memory and use that for the weight.
		1
	}
}
