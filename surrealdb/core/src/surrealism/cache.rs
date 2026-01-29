use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::Result;
use priority_lfu::CacheKey;
use surrealism_runtime::controller::Runtime;

use crate::catalog::{DatabaseId, NamespaceId};

pub struct SurrealismCache {
	cache: priority_lfu::Cache,
}

impl SurrealismCache {
	pub fn new() -> Self {
		Self {
			cache: priority_lfu::Cache::new(*crate::cnf::SURREALISM_CACHE_SIZE),
		}
	}

	pub fn remove(&self, lookup: &SurrealismCacheLookup) {
		self.cache.remove(&lookup.to_key());
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
		let key = lookup.to_key();

		// Try fast path first
		if let Some(runtime) = self.cache.get_clone(&key) {
			return Ok(runtime);
		}

		// Slow path: compute and insert
		// Note: There's a potential race condition here where multiple concurrent
		// calls might compute the same value. This is acceptable as the cache
		// will use one of the computed values.
		let runtime = compute().await?;
		self.cache.insert(key, Arc::clone(&runtime));
		Ok(runtime)
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub enum SurrealismCacheKey {
	// NS - DB - BUCKET - KEY
	File(NamespaceId, DatabaseId, String, String),
	// Organisation - Package - MAJOR - MINOR - PATCH
	Silo(String, String, u32, u32, u32),
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub enum SurrealismCacheLookup<'a> {
	// NS - DB - BUCKET - KEY
	File(&'a NamespaceId, &'a DatabaseId, &'a str, &'a str),
	// Organisation - Package - MAJOR - MINOR - PATCH
	Silo(&'a str, &'a str, u32, u32, u32),
}

impl SurrealismCacheLookup<'_> {
	pub fn to_key(&self) -> SurrealismCacheKey {
		match self {
			SurrealismCacheLookup::File(ns, db, bucket, key) => {
				SurrealismCacheKey::File(**ns, **db, (*bucket).to_string(), (*key).to_string())
			}
			SurrealismCacheLookup::Silo(org, pkg, maj, min, pat) => {
				SurrealismCacheKey::Silo((*org).to_string(), (*pkg).to_string(), *maj, *min, *pat)
			}
		}
	}
}

impl<'a> From<SurrealismCacheLookup<'a>> for SurrealismCacheKey {
	fn from(lookup: SurrealismCacheLookup<'a>) -> Self {
		lookup.to_key()
	}
}

// Implement CacheKey to link the key type to the value type
impl CacheKey for SurrealismCacheKey {
	type Value = Arc<Runtime>;
}
