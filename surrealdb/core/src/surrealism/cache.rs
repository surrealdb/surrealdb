use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Error, Result};
use quick_cache::{Equivalent, Weighter};
use surrealism_runtime::controller::Runtime;

use crate::catalog::{DatabaseId, NamespaceId};

pub struct SurrealismCache {
	cache: quick_cache::sync::Cache<SurrealismCacheKey, SurrealismCacheValue, Weight>,
}

impl SurrealismCache {
	pub fn new(capacity: usize) -> Self {
		Self {
			cache: quick_cache::sync::Cache::with_weighter(capacity, capacity as u64, Weight),
		}
	}

	pub fn remove(&self, lookup: &SurrealismCacheLookup) {
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
		// This match is only needed to avoid allocating for the key in the fast path
		let value = match self.cache.get(lookup) {
			Some(runtime) => runtime,
			None => {
				let compute = async {
					let value = SurrealismCacheValue {
						runtime: compute().await?,
					};
					Result::<_, Error>::Ok(value)
				};

				self.cache.get_or_insert_async(&lookup.to_key(), compute).await?
			}
		};

		Ok(value.runtime)
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

impl Equivalent<SurrealismCacheKey> for SurrealismCacheLookup<'_> {
	fn equivalent(&self, key: &SurrealismCacheKey) -> bool {
		match (self, key) {
			(Self::File(a1, b1, c1, d1), SurrealismCacheKey::File(a2, b2, c2, d2)) => {
				a1.0 == a2.0 && b1.0 == b2.0 && c1 == c2 && d1 == d2
			}
			(Self::Silo(a1, b1, c1, d1, e1), SurrealismCacheKey::Silo(a2, b2, c2, d2, e2)) => {
				a1 == a2 && b1 == b2 && c1 == c2 && d1 == d2 && e1 == e2
			}
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
