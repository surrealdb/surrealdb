use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;
use quick_cache::{Equivalent, Weighter};
use std::hash::Hash;
use surrealism_runtime::controller::Runtime;
use tokio::sync::Mutex;

use crate::catalog::{DatabaseId, NamespaceId};

pub struct SurrealismCache {
	cache: quick_cache::sync::Cache<SurrealismCacheKey, SurrealismCacheValue, Weight>,
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

	pub fn get<K: Equivalent<SurrealismCacheKey> + Hash>(
		&self,
		lookup: &K,
	) -> Option<SurrealismCacheValue> {
		self.cache.get(lookup)
	}

	pub fn remove<K: Equivalent<SurrealismCacheKey> + Hash>(&self, lookup: &K) {
		self.cache.remove(lookup);
	}

	/// Gets the runtime from the cache or computes it if not present using the provided function
	pub async fn get_or_insert_with<F, Fut>(
		&self,
		lookup: &SurrealismCacheLookup<'_>,
		compute: F,
	) -> Result<Arc<Runtime>>
	where
		F: FnOnce() -> Fut,
		Fut: Future<Output = Result<Arc<Runtime>>>,
	{
		// if already cached, return it
		if let Some(value) = self.get(lookup) {
			return Ok(value.runtime);
		}

		// if not cached, acquire the loading lock
		let cache_key: SurrealismCacheKey = lookup.clone().into();
		let loading_lock = self.get_loading_lock(&cache_key);
		let _guard = loading_lock.lock().await;

		// double-check if it got cached while waiting for the lock
		if let Some(value) = self.get(&cache_key) {
			self.remove_loading_lock(&cache_key);
			return Ok(value.runtime);
		}

		// compute the runtime
		let result = compute().await;

		// if successful, insert into cache
		if let Ok(runtime) = &result {
			self.cache.insert(
				cache_key.clone(),
				SurrealismCacheValue {
					runtime: runtime.clone(),
				},
			);
		}

		self.remove_loading_lock(&cache_key);

		result
	}

	fn get_loading_lock(&self, key: &SurrealismCacheKey) -> Arc<Mutex<()>> {
		match self.loading.get(key) {
			Some(lock) => lock.clone(),
			None => {
				let lock = Arc::new(Mutex::new(()));
				self.loading.insert(key.clone(), lock.clone());
				lock
			}
		}
	}

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

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
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
