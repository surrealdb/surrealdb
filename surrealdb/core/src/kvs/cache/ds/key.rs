use std::sync::Arc;

use priority_lfu::{CacheKey, CachePolicy};
use uuid::Uuid;

use crate::catalog::{
	DatabaseDefinition, DatabaseId, EventDefinition, IndexDefinition, NamespaceId,
	SubscriptionDefinition, TableDefinition,
};
use crate::val::TableName;

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DbCacheKey(pub String, pub String);

impl CacheKey for DbCacheKey {
	type Value = Arc<DatabaseDefinition>;

	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ForiegnTablesCacheKey(pub NamespaceId, pub DatabaseId, pub TableName);

impl CacheKey for ForiegnTablesCacheKey {
	type Value = Arc<[TableDefinition]>;

	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct EventsCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub Uuid);

impl CacheKey for EventsCacheKey {
	type Value = Arc<[EventDefinition]>;

	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct IndexesCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub Uuid);

impl CacheKey for IndexesCacheKey {
	type Value = Arc<[IndexDefinition]>;

	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct LiveQueriesCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub Uuid);

impl CacheKey for LiveQueriesCacheKey {
	type Value = Arc<[SubscriptionDefinition]>;

	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct LiveQueriesVersionCacheKey(pub NamespaceId, pub DatabaseId, pub TableName);

impl CacheKey for LiveQueriesVersionCacheKey {
	type Value = Uuid;

	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}
