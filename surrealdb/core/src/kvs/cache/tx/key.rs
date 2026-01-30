use std::sync::Arc;

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

impl_cache_key!(NodesCacheKey, Arc<[Node]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RootUsersCacheKey;

impl_cache_key!(RootUsersCacheKey, Arc<[UserDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RootAccessesCacheKey;

impl_cache_key!(RootAccessesCacheKey, Arc<[AccessDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct RootAccessGrantsCacheKey(pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct RootAccessGrantsCacheKeyRef<'a>(pub &'a str);

impl_cache_key!(RootAccessGrantsCacheKey, Arc<[AccessGrant]>, Critical);
impl_cache_key_lookup!(RootAccessGrantsCacheKeyRef<'a> => RootAccessGrantsCacheKey {
	0 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NamespacesCacheKey;

impl_cache_key!(NamespacesCacheKey, Arc<[NamespaceDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NamespaceUsersCacheKey(pub NamespaceId);

impl_cache_key!(NamespaceUsersCacheKey, Arc<[UserDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NamespaceAccessesCacheKey(pub NamespaceId);

impl_cache_key!(NamespaceAccessesCacheKey, Arc<[AccessDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct NamespaceAccessGrantsCacheKey(pub NamespaceId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct NamespaceAccessGrantsCacheKeyRef<'a>(pub NamespaceId, pub &'a str);

impl_cache_key!(NamespaceAccessGrantsCacheKey, Arc<[AccessGrant]>, Critical);
impl_cache_key_lookup!(NamespaceAccessGrantsCacheKeyRef<'a> => NamespaceAccessGrantsCacheKey {
	0 => copy,
	1 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DatabasesCacheKey(pub NamespaceId);

impl_cache_key!(DatabasesCacheKey, Arc<[DatabaseDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DatabaseUsersCacheKey(pub NamespaceId, pub DatabaseId);

impl_cache_key!(DatabaseUsersCacheKey, Arc<[UserDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DatabaseAccessesCacheKey(pub NamespaceId, pub DatabaseId);

impl_cache_key!(DatabaseAccessesCacheKey, Arc<[AccessDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct DatabaseAccessGrantsCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct DatabaseAccessGrantsCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(DatabaseAccessGrantsCacheKey, Arc<[AccessGrant]>, Critical);
impl_cache_key_lookup!(DatabaseAccessGrantsCacheKeyRef<'a> => DatabaseAccessGrantsCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ApisCacheKey(pub NamespaceId, pub DatabaseId);

impl_cache_key!(ApisCacheKey, Arc<[ApiDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct AnalyzersCacheKey(pub NamespaceId, pub DatabaseId);

impl_cache_key!(AnalyzersCacheKey, Arc<[AnalyzerDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct BucketsCacheKey(pub NamespaceId, pub DatabaseId);

impl_cache_key!(BucketsCacheKey, Arc<[BucketDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct FunctionsCacheKey(pub NamespaceId, pub DatabaseId);

impl_cache_key!(FunctionsCacheKey, Arc<[FunctionDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ModulesCacheKey(pub NamespaceId, pub DatabaseId);

impl_cache_key!(ModulesCacheKey, Arc<[ModuleDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ModelsCacheKey(pub NamespaceId, pub DatabaseId);

impl_cache_key!(ModelsCacheKey, Arc<[MlModelDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ConfigsCacheKey(pub NamespaceId, pub DatabaseId);

impl_cache_key!(ConfigsCacheKey, Arc<[ConfigDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ParamsCacheKey(pub NamespaceId, pub DatabaseId);

impl_cache_key!(ParamsCacheKey, Arc<[ParamDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TablesCacheKey(pub NamespaceId, pub DatabaseId);

impl_cache_key!(TablesCacheKey, Arc<[TableDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct SequencesCacheKey(pub NamespaceId, pub DatabaseId);

impl_cache_key!(SequencesCacheKey, Arc<[SequenceDefinition]>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableEventsCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableEventsCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(TableEventsCacheKey, Arc<[EventDefinition]>, Critical);
impl_cache_key_lookup!(TableEventsCacheKeyRef<'a> => TableEventsCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableFieldsCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableFieldsCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(TableFieldsCacheKey, Arc<[FieldDefinition]>, Critical);
impl_cache_key_lookup!(TableFieldsCacheKeyRef<'a> => TableFieldsCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableViewsCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableViewsCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(TableViewsCacheKey, Arc<[TableDefinition]>, Critical);
impl_cache_key_lookup!(TableViewsCacheKeyRef<'a> => TableViewsCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableIndexesCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableIndexesCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(TableIndexesCacheKey, Arc<[IndexDefinition]>, Critical);
impl_cache_key_lookup!(TableIndexesCacheKeyRef<'a> => TableIndexesCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableLivesCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableLivesCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(TableLivesCacheKey, Arc<[SubscriptionDefinition]>, Critical);
impl_cache_key_lookup!(TableLivesCacheKeyRef<'a> => TableLivesCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

// Single item lookups (specific entities)

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NodeCacheKey(pub Uuid);

impl_cache_key!(NodeCacheKey, Arc<Node>, Critical);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RootConfigCacheKey(pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RootConfigCacheKeyRef<'a>(pub &'a str);

impl_cache_key!(RootConfigCacheKey, Arc<ConfigDefinition>, Critical);
impl_cache_key_lookup!(RootConfigCacheKeyRef<'a> => RootConfigCacheKey {
	0 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RootUserCacheKey(pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RootUserCacheKeyRef<'a>(pub &'a str);

impl_cache_key!(RootUserCacheKey, Arc<UserDefinition>, Critical);
impl_cache_key_lookup!(RootUserCacheKeyRef<'a> => RootUserCacheKey {
	0 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RootAccessCacheKey(pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RootAccessCacheKeyRef<'a>(pub &'a str);

impl_cache_key!(RootAccessCacheKey, Arc<AccessDefinition>, Critical);
impl_cache_key_lookup!(RootAccessCacheKeyRef<'a> => RootAccessCacheKey {
	0 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct RootAccessGrantCacheKey(pub String, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct RootAccessGrantCacheKeyRef<'a>(pub &'a str, pub &'a str);

impl_cache_key!(RootAccessGrantCacheKey, Arc<AccessGrant>, Critical);
impl_cache_key_lookup!(RootAccessGrantCacheKeyRef<'a> => RootAccessGrantCacheKey {
	0 => to_owned,
	1 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NamespaceByNameCacheKey(pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NamespaceByNameCacheKeyRef<'a>(pub &'a str);

impl_cache_key!(NamespaceByNameCacheKey, Arc<NamespaceDefinition>, Critical);
impl_cache_key_lookup!(NamespaceByNameCacheKeyRef<'a> => NamespaceByNameCacheKey {
	0 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NamespaceUserCacheKey(pub NamespaceId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NamespaceUserCacheKeyRef<'a>(pub NamespaceId, pub &'a str);

impl_cache_key!(NamespaceUserCacheKey, Arc<UserDefinition>, Critical);
impl_cache_key_lookup!(NamespaceUserCacheKeyRef<'a> => NamespaceUserCacheKey {
	0 => copy,
	1 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NamespaceAccessCacheKey(pub NamespaceId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NamespaceAccessCacheKeyRef<'a>(pub NamespaceId, pub &'a str);

impl_cache_key!(NamespaceAccessCacheKey, Arc<AccessDefinition>, Critical);
impl_cache_key_lookup!(NamespaceAccessCacheKeyRef<'a> => NamespaceAccessCacheKey {
	0 => copy,
	1 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct NamespaceAccessGrantCacheKey(pub NamespaceId, pub String, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct NamespaceAccessGrantCacheKeyRef<'a>(pub NamespaceId, pub &'a str, pub &'a str);

impl_cache_key!(NamespaceAccessGrantCacheKey, Arc<AccessGrant>, Critical);
impl_cache_key_lookup!(NamespaceAccessGrantCacheKeyRef<'a> => NamespaceAccessGrantCacheKey {
	0 => copy,
	1 => to_owned,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DatabaseByNameCacheKey(pub String, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DatabaseByNameCacheKeyRef<'a>(pub &'a str, pub &'a str);

impl_cache_key!(DatabaseByNameCacheKey, Arc<DatabaseDefinition>, Critical);
impl_cache_key_lookup!(DatabaseByNameCacheKeyRef<'a> => DatabaseByNameCacheKey {
	0 => to_owned,
	1 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DatabaseUserCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DatabaseUserCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(DatabaseUserCacheKey, Arc<UserDefinition>, Critical);
impl_cache_key_lookup!(DatabaseUserCacheKeyRef<'a> => DatabaseUserCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DatabaseAccessCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct DatabaseAccessCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(DatabaseAccessCacheKey, Arc<AccessDefinition>, Critical);
impl_cache_key_lookup!(DatabaseAccessCacheKeyRef<'a> => DatabaseAccessCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct DatabaseAccessGrantCacheKey(
	pub NamespaceId,
	pub DatabaseId,
	pub String,
	pub String,
);

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct DatabaseAccessGrantCacheKeyRef<'a>(
	pub NamespaceId,
	pub DatabaseId,
	pub &'a str,
	pub &'a str,
);

impl_cache_key!(DatabaseAccessGrantCacheKey, Arc<AccessGrant>, Critical);
impl_cache_key_lookup!(DatabaseAccessGrantCacheKeyRef<'a> => DatabaseAccessGrantCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
	3 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ApiCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ApiCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(ApiCacheKey, Arc<ApiDefinition>, Critical);
impl_cache_key_lookup!(ApiCacheKeyRef<'a> => ApiCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct AnalyzerCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct AnalyzerCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(AnalyzerCacheKey, Arc<AnalyzerDefinition>, Critical);
impl_cache_key_lookup!(AnalyzerCacheKeyRef<'a> => AnalyzerCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct BucketCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct BucketCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(BucketCacheKey, Arc<BucketDefinition>, Critical);
impl_cache_key_lookup!(BucketCacheKeyRef<'a> => BucketCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct FunctionCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct FunctionCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(FunctionCacheKey, Arc<FunctionDefinition>, Critical);
impl_cache_key_lookup!(FunctionCacheKeyRef<'a> => FunctionCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ModuleCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ModuleCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(ModuleCacheKey, Arc<ModuleDefinition>, Critical);
impl_cache_key_lookup!(ModuleCacheKeyRef<'a> => ModuleCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ModelCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ModelCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str, pub &'a str);

impl_cache_key!(ModelCacheKey, Arc<MlModelDefinition>, Critical);
impl_cache_key_lookup!(ModelCacheKeyRef<'a> => ModelCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
	3 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ConfigCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ConfigCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(ConfigCacheKey, Arc<ConfigDefinition>, Critical);
impl_cache_key_lookup!(ConfigCacheKeyRef<'a> => ConfigCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ParamCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ParamCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(ParamCacheKey, Arc<ParamDefinition>, Critical);
impl_cache_key_lookup!(ParamCacheKeyRef<'a> => ParamCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct SequenceCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct SequenceCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(SequenceCacheKey, Arc<SequenceDefinition>, Critical);
impl_cache_key_lookup!(SequenceCacheKeyRef<'a> => SequenceCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableByNameCacheKey(pub String, pub String, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableByNameCacheKeyRef<'a>(pub &'a str, pub &'a str, pub &'a str);

impl_cache_key!(TableByNameCacheKey, Arc<TableDefinition>, Critical);
impl_cache_key_lookup!(TableByNameCacheKeyRef<'a> => TableByNameCacheKey {
	0 => to_owned,
	1 => to_owned,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableCacheKey(pub NamespaceId, pub DatabaseId, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str);

impl_cache_key!(TableCacheKey, Arc<TableDefinition>, Critical);
impl_cache_key_lookup!(TableCacheKeyRef<'a> => TableCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct EventCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct EventCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str, pub &'a str);

impl_cache_key!(EventCacheKey, Arc<EventDefinition>, Critical);
impl_cache_key_lookup!(EventCacheKeyRef<'a> => EventCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
	3 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct FieldCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct FieldCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str, pub &'a str);

impl_cache_key!(FieldCacheKey, Arc<FieldDefinition>, Critical);
impl_cache_key_lookup!(FieldCacheKeyRef<'a> => FieldCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
	3 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct IndexCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub String);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct IndexCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str, pub &'a str);

impl_cache_key!(IndexCacheKey, Arc<IndexDefinition>, Critical);
impl_cache_key_lookup!(IndexCacheKeyRef<'a> => IndexCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
	3 => to_owned,
});

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RecordCacheKey(pub NamespaceId, pub DatabaseId, pub String, pub RecordIdKey);

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RecordCacheKeyRef<'a>(pub NamespaceId, pub DatabaseId, pub &'a str, pub &'a RecordIdKey);

impl_cache_key!(RecordCacheKey, Arc<Record>, Volatile);
impl_cache_key_lookup!(RecordCacheKeyRef<'a> => RecordCacheKey {
	0 => copy,
	1 => copy,
	2 => to_owned,
	3 => to_owned,
});
