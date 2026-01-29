use std::sync::Arc;

use priority_lfu::{CacheKey, CachePolicy};
use uuid::Uuid;

use crate::catalog::{
	AccessDefinition, AccessGrant, AnalyzerDefinition, ApiDefinition, BucketDefinition,
	ConfigDefinition, DatabaseDefinition, DatabaseId, EventDefinition, FieldDefinition,
	FunctionDefinition, IndexDefinition, MlModelDefinition, ModuleDefinition, NamespaceDefinition,
	NamespaceId, ParamDefinition, Record, SequenceDefinition, SubscriptionDefinition,
	TableDefinition, UserDefinition,
};
use crate::dbs::node::Node;
use crate::val::RecordIdKey;

// Root level collections (all items)

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NodesCacheKey;

impl CacheKey for NodesCacheKey {
	type Value = Arc<[Node]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RootUsersCacheKey;

impl CacheKey for RootUsersCacheKey {
	type Value = Arc<[UserDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RootAccessesCacheKey;

impl CacheKey for RootAccessesCacheKey {
	type Value = Arc<[AccessDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct RootAccessGrantsCacheKey(pub String);

impl CacheKey for RootAccessGrantsCacheKey {
	type Value = Arc<[AccessGrant]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NamespacesCacheKey;

impl CacheKey for NamespacesCacheKey {
	type Value = Arc<[NamespaceDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NamespaceUsersCacheKey(pub NamespaceId);

impl CacheKey for NamespaceUsersCacheKey {
	type Value = Arc<[UserDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NamespaceAccessesCacheKey(pub NamespaceId);

impl CacheKey for NamespaceAccessesCacheKey {
	type Value = Arc<[AccessDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct NamespaceAccessGrantsCacheKey(pub NamespaceId, pub String);

impl CacheKey for NamespaceAccessGrantsCacheKey {
	type Value = Arc<[AccessGrant]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DatabasesCacheKey(pub NamespaceId);

impl CacheKey for DatabasesCacheKey {
	type Value = Arc<[DatabaseDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DatabaseUsersCacheKey(pub NamespaceId, pub DatabaseId);

impl CacheKey for DatabaseUsersCacheKey {
	type Value = Arc<[UserDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DatabaseAccessesCacheKey(pub NamespaceId, pub DatabaseId);

impl CacheKey for DatabaseAccessesCacheKey {
	type Value = Arc<[AccessDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct DatabaseAccessGrantsCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for DatabaseAccessGrantsCacheKey {
	type Value = Arc<[AccessGrant]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ApisCacheKey(pub NamespaceId, pub DatabaseId);

impl CacheKey for ApisCacheKey {
	type Value = Arc<[ApiDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct AnalyzersCacheKey(pub NamespaceId, pub DatabaseId);

impl CacheKey for AnalyzersCacheKey {
	type Value = Arc<[AnalyzerDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct BucketsCacheKey(pub NamespaceId, pub DatabaseId);

impl CacheKey for BucketsCacheKey {
	type Value = Arc<[BucketDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct FunctionsCacheKey(pub NamespaceId, pub DatabaseId);

impl CacheKey for FunctionsCacheKey {
	type Value = Arc<[FunctionDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ModulesCacheKey(pub NamespaceId, pub DatabaseId);

impl CacheKey for ModulesCacheKey {
	type Value = Arc<[ModuleDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ModelsCacheKey(pub NamespaceId, pub DatabaseId);

impl CacheKey for ModelsCacheKey {
	type Value = Arc<[MlModelDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ConfigsCacheKey(pub NamespaceId, pub DatabaseId);

impl CacheKey for ConfigsCacheKey {
	type Value = Arc<[ConfigDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ParamsCacheKey(pub NamespaceId, pub DatabaseId);

impl CacheKey for ParamsCacheKey {
	type Value = Arc<[ParamDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TablesCacheKey(pub NamespaceId, pub DatabaseId);

impl CacheKey for TablesCacheKey {
	type Value = Arc<[TableDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct SequencesCacheKey(pub NamespaceId, pub DatabaseId);

impl CacheKey for SequencesCacheKey {
	type Value = Arc<[SequenceDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableEventsCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for TableEventsCacheKey {
	type Value = Arc<[EventDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableFieldsCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for TableFieldsCacheKey {
	type Value = Arc<[FieldDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableViewsCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for TableViewsCacheKey {
	type Value = Arc<[TableDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableIndexesCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for TableIndexesCacheKey {
	type Value = Arc<[IndexDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableLivesCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for TableLivesCacheKey {
	type Value = Arc<[SubscriptionDefinition]>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

// Single item lookups (specific entities)

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NodeCacheKey(pub Uuid);

impl CacheKey for NodeCacheKey {
	type Value = Arc<Node>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RootConfigCacheKey(pub String);

impl CacheKey for RootConfigCacheKey {
	type Value = Arc<ConfigDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RootUserCacheKey(pub String);

impl CacheKey for RootUserCacheKey {
	type Value = Arc<UserDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RootAccessCacheKey(pub String);

impl CacheKey for RootAccessCacheKey {
	type Value = Arc<AccessDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct RootAccessGrantCacheKey(pub String, pub String);

impl CacheKey for RootAccessGrantCacheKey {
	type Value = Arc<AccessGrant>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NamespaceByNameCacheKey(pub String);

impl CacheKey for NamespaceByNameCacheKey {
	type Value = Arc<NamespaceDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NamespaceUserCacheKey(pub NamespaceId, pub String);

impl CacheKey for NamespaceUserCacheKey {
	type Value = Arc<UserDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NamespaceAccessCacheKey(pub NamespaceId, pub String);

impl CacheKey for NamespaceAccessCacheKey {
	type Value = Arc<AccessDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct NamespaceAccessGrantCacheKey(pub NamespaceId, pub String, pub String);

impl CacheKey for NamespaceAccessGrantCacheKey {
	type Value = Arc<AccessGrant>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DatabaseByNameCacheKey(pub String, pub String);

impl CacheKey for DatabaseByNameCacheKey {
	type Value = Arc<DatabaseDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DatabaseUserCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for DatabaseUserCacheKey {
	type Value = Arc<UserDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DatabaseAccessCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for DatabaseAccessCacheKey {
	type Value = Arc<AccessDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct DatabaseAccessGrantCacheKey(
	pub NamespaceId,
	pub DatabaseId,
	pub String,
	pub String,
);

impl CacheKey for DatabaseAccessGrantCacheKey {
	type Value = Arc<AccessGrant>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ApiCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for ApiCacheKey {
	type Value = Arc<ApiDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct AnalyzerCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for AnalyzerCacheKey {
	type Value = Arc<AnalyzerDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct BucketCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for BucketCacheKey {
	type Value = Arc<BucketDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct FunctionCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for FunctionCacheKey {
	type Value = Arc<FunctionDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ModuleCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for ModuleCacheKey {
	type Value = Arc<ModuleDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ModelCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub String);

impl CacheKey for ModelCacheKey {
	type Value = Arc<MlModelDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ConfigCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for ConfigCacheKey {
	type Value = Arc<ConfigDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ParamCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for ParamCacheKey {
	type Value = Arc<ParamDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct SequenceCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for SequenceCacheKey {
	type Value = Arc<SequenceDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableByNameCacheKey(pub String, pub String, pub String);

impl CacheKey for TableByNameCacheKey {
	type Value = Arc<TableDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableCacheKey(pub NamespaceId, pub DatabaseId, pub String);

impl CacheKey for TableCacheKey {
	type Value = Arc<TableDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct EventCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub String);

impl CacheKey for EventCacheKey {
	type Value = Arc<EventDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct FieldCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub String);

impl CacheKey for FieldCacheKey {
	type Value = Arc<FieldDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct IndexCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub String);

impl CacheKey for IndexCacheKey {
	type Value = Arc<IndexDefinition>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Critical
	}
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RecordCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub RecordIdKey);

impl CacheKey for RecordCacheKey {
	type Value = Arc<Record>;
	fn policy(&self) -> CachePolicy {
		CachePolicy::Volatile
	}
}
