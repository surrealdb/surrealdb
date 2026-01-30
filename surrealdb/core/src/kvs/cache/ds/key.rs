use std::sync::Arc;

use uuid::Uuid;

use crate::catalog::{
	DatabaseDefinition, DatabaseId, EventDefinition, IndexDefinition, NamespaceId,
	SubscriptionDefinition, TableDefinition,
};
use crate::val::TableName;

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DbCacheKey(pub String, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DbCacheKeyRef<'a>(pub &'a str, pub &'a str);

impl_cache_key!(DbCacheKey, Arc<DatabaseDefinition>, Critical);
impl_cache_key_lookup!(DbCacheKeyRef<'a> => DbCacheKey {
	0 => to_owned,
	1 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ForiegnTablesCacheKey(pub NamespaceId, pub DatabaseId, pub TableName);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ForiegnTablesCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a TableName);

impl_cache_key!(ForiegnTablesCacheKey, Arc<[TableDefinition]>, Critical);
impl_cache_key_lookup!(ForiegnTablesCacheKeyRef<'a> => ForiegnTablesCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct EventsCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub Uuid);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct EventsCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str, pub Uuid);

impl_cache_key!(EventsCacheKey, Arc<[EventDefinition]>, Critical);
impl_cache_key_lookup!(EventsCacheKeyRef<'a> => EventsCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
	3 => copy,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct IndexesCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub Uuid);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct IndexesCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str, pub Uuid);

impl_cache_key!(IndexesCacheKey, Arc<[IndexDefinition]>, Critical);
impl_cache_key_lookup!(IndexesCacheKeyRef<'a> => IndexesCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
	3 => copy,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct LiveQueriesCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub Uuid);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct LiveQueriesCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str, pub Uuid);

impl_cache_key!(LiveQueriesCacheKey, Arc<[SubscriptionDefinition]>, Critical);
impl_cache_key_lookup!(LiveQueriesCacheKeyRef<'a> => LiveQueriesCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
	3 => copy,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct LiveQueriesVersionCacheKey(pub NamespaceId, pub DatabaseId, pub TableName);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct LiveQueriesVersionCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a TableName);

impl_cache_key!(LiveQueriesVersionCacheKey, Uuid, Critical);
impl_cache_key_lookup!(LiveQueriesVersionCacheKeyRef<'a> => LiveQueriesVersionCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[cfg(test)]
mod tests {
	use std::hash::{DefaultHasher, Hash, Hasher};

	use priority_lfu::CacheKeyLookup;
	use rstest::rstest;

	use super::*;

	fn hash<T: Hash>(value: &T) -> u64 {
		let mut hasher = DefaultHasher::new();
		value.hash(&mut hasher);
		hasher.finish()
	}

	#[rstest]
	#[case(DbCacheKeyRef("test-ns", "test-db"))]
	fn test_hash_equality(#[case] lookup: DbCacheKeyRef<'_>) {
		let key = lookup.clone().to_owned_key();
		// calculate the hash of the lookup and key
		let lookup_hash = hash(&lookup);
		let key_hash = hash(&key);
		assert_eq!(lookup_hash, key_hash);
	}
}
