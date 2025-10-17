use quick_cache::Equivalent;
use surrealism_runtime::controller::Runtime;
use crate::{catalog::{DatabaseId, NamespaceId}, val::File};
use quick_cache::Weighter;
use std::sync::Arc;


pub struct SurrealismCache {
    cache: quick_cache::sync::Cache<SurrealismCacheKey, SurrealismCacheValue, Weight>,
}

impl SurrealismCache {
    pub fn new() -> Self {
        Self {
            cache: quick_cache::sync::Cache::with_weighter(
                *crate::cnf::SURREALISM_CACHE_SIZE,
                *crate::cnf::SURREALISM_CACHE_SIZE as u64,
                Weight,
            ),
        }
    }

    pub fn get(&self, lookup: &SurrealismCacheLookup) -> Option<SurrealismCacheValue> {
        self.cache.get(lookup)
    }

    pub fn insert(&self, key: SurrealismCacheKey, value: SurrealismCacheValue) {
        self.cache.insert(key, value);
    }
}

#[derive(Hash, Eq, PartialEq)]
pub enum SurrealismCacheKey {
    // NS - DB - FILE
    File(NamespaceId, DatabaseId, File),
    // Organisation - Package - MAJOR - MINOR - PATCH
    Silo(String, String, u32, u32, u32),
}

#[derive(Hash, Eq, PartialEq)]
pub enum SurrealismCacheLookup<'a> {
    // NS - DB - FILE
    File(&'a NamespaceId, &'a DatabaseId, &'a File),
    // Organisation - Package - MAJOR - MINOR - PATCH
    Silo(&'a str, &'a str, u32, u32, u32),
}

impl<'a> From<SurrealismCacheLookup<'a>> for SurrealismCacheKey {
    fn from(lookup: SurrealismCacheLookup<'a>) -> Self {
        match lookup {
            SurrealismCacheLookup::File(ns, db, file) => SurrealismCacheKey::File(*ns, *db, file.to_owned()),
            SurrealismCacheLookup::Silo(org, pkg, maj, min, pat) => SurrealismCacheKey::Silo(org.to_string(), pkg.to_string(), maj, min, pat),
        }
    }
}

impl Equivalent<SurrealismCacheKey> for SurrealismCacheLookup<'_> {
    #[rustfmt::skip]
    fn equivalent(&self, key: &SurrealismCacheKey) -> bool {
        match (self, key) {
            (Self::File(a1, b1, c1), SurrealismCacheKey::File(a2, b2, c2)) 
                => a1.0 == a2.0 && b1.0 == b2.0 && c1 == c2,
            (Self::Silo(a1, b1, c1, d1, e1), SurrealismCacheKey::Silo(a2, b2, c2, d2, e2)) 
                => a1 == a2 && b1 == b2 && c1 == c2 && d1 == d2 && e1 == e2,
            _ => false,
        }
    }
}

#[derive(Clone)]
pub struct SurrealismCacheValue {
    pub(crate) runtime: Arc<Runtime>
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