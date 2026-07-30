use anyhow::Context as _;
use revision::{
	DeserializeRevisioned, Revisioned, SerializeRevisioned, SkipRevisioned, revisioned,
};
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};
use uuid::Uuid;

use crate::catalog::{
	DatabaseId, FromStored, NamespaceId, Permission, Permissions, StoredPermissions,
	StoredViewDefinition, ViewDefinition,
};
use crate::expr::ChangeFeed;
use crate::expr::statements::info::InfoStructure;
pub use crate::expr::table_type::{Relation, TableType};
use crate::sql;
use crate::sql::statements::DefineTableStatement;
use crate::val::{TableName, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct TableId(pub u32);

impl_kv_value_revisioned!(TableId);

impl Revisioned for TableId {
	fn revision() -> u16 {
		1
	}
}

impl SerializeRevisioned for TableId {
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		SerializeRevisioned::serialize_revisioned(&self.0, writer)
	}
}

impl DeserializeRevisioned for TableId {
	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		DeserializeRevisioned::deserialize_revisioned(reader).map(TableId)
	}
}

impl SkipRevisioned for TableId {
	#[inline]
	fn skip_revisioned<R: std::io::Read>(reader: &mut R) -> Result<(), revision::Error> {
		<u32 as SkipRevisioned>::skip_revisioned(reader)
	}
}

impl revision::WalkRevisioned for TableId {
	type Walker<'r, R: revision::BorrowedReader + 'r> = revision::LeafWalker<'r, TableId, R>;

	#[inline]
	fn walk_revisioned<'r, R: revision::BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, revision::Error> {
		Ok(revision::LeafWalker::new(reader))
	}
}

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StoredTableDefinition {
	pub namespace_id: NamespaceId,
	pub database_id: DatabaseId,
	pub table_id: TableId,
	pub name: Strand,
	pub drop: bool,
	pub schemafull: bool,
	pub view: Option<StoredViewDefinition>,
	pub permissions: StoredPermissions,
	pub changefeed: Option<ChangeFeed>,
	pub comment: Option<String>,
	pub table_type: TableType,

	/// The last time that a DEFINE FIELD was added to this table
	pub cache_fields_ts: Uuid,
	/// The last time that a DEFINE EVENT was added to this table
	pub cache_events_ts: Uuid,
	/// The last time that a DEFINE TABLE was added to this table
	pub cache_tables_ts: Uuid,
	/// The last time that a DEFINE INDEX was added to this table
	pub cache_indexes_ts: Uuid,

	/// The last time the set of LIVE queries on this table changed (a LIVE was
	/// registered or a KILL removed one). Bumped transactionally with the
	/// live-query row write, so the live-query cache keys on committed state —
	/// exactly like `cache_fields_ts` etc. A free-floating in-memory version
	/// (bumped before commit) allowed a concurrent writer with a pre-commit
	/// snapshot to poison the cache with a stale subscriber list. Old tables
	/// default to the nil UUID; the first LIVE/KILL bumps it to a real value.
	#[revision(start = 3)]
	pub cache_lives_ts: Uuid,

	/// Optional alias used as the GraphQL type / query / mutation prefix for
	/// this table. See GitHub issue #4537. `Option<String>::default()` is
	/// `None`, so the standard `#[revision]` default is sufficient.
	#[revision(start = 2)]
	pub graphql_alias: Option<String>,

	/// Reason emitted on the GraphQL `@deprecated` directive for every
	/// auto-generated Query/Mutation field that targets this table.
	#[revision(start = 2)]
	pub graphql_deprecated: Option<String>,
}

impl_kv_value_revisioned!(StoredTableDefinition);

impl StoredTableDefinition {
	pub fn new(
		namespace_id: NamespaceId,
		database_id: DatabaseId,
		table_id: TableId,
		name: TableName,
	) -> Self {
		let now = Uuid::now_v7();
		Self {
			namespace_id,
			database_id,
			table_id,
			name: name.into(),
			drop: false,
			schemafull: false,
			view: None,
			permissions: StoredPermissions::none(),
			changefeed: None,
			comment: None,
			table_type: TableType::default(),
			cache_fields_ts: now,
			cache_events_ts: now,
			cache_tables_ts: now,
			cache_indexes_ts: now,
			cache_lives_ts: now,
			graphql_alias: None,
			graphql_deprecated: None,
		}
	}
}

impl TableDefinition {
	fn to_sql_definition(&self) -> DefineTableStatement {
		DefineTableStatement {
			id: Some(self.table_id.0),
			name: sql::Expr::Table(self.name.clone().into()),
			drop: self.drop,
			full: self.schemafull,
			view: self.view.as_ref().map(|v| v.to_sql_definition()),
			permissions: self.permissions.to_sql_permissions(),
			changefeed: self.changefeed.map(|v| v.into()),
			comment: self
				.comment
				.clone()
				.map(|v| sql::Expr::Literal(sql::Literal::String(v.into())))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
			table_type: self.table_type.clone().into(),
			graphql_alias: self.graphql_alias.clone(),
			graphql_deprecated: self.graphql_deprecated.clone(),
			..Default::default()
		}
	}
}

impl ToSql for TableDefinition {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, sql_fmt)
	}
}

impl InfoStructure for TableDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name" => Value::String(self.name.into()),
			"drop" => self.drop.into(),
			"schemafull" => self.schemafull.into(),
			"kind" => self.table_type.structure(),
			"view", if let Some(v) = self.view => v.structure(),
			"changefeed", if let Some(v) = self.changefeed => v.structure(),
			"permissions" => self.permissions.structure(),
			"comment", if let Some(v) = self.comment => v.into(),
			"graphql_alias", if let Some(v) = self.graphql_alias => v.into(),
			"graphql_deprecated", if let Some(v) = self.graphql_deprecated => v.into(),
			"id" => self.table_id.0.into(),
		})
	}
}

/// Runtime form of [`StoredTableDefinition`].
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TableDefinition {
	pub namespace_id: NamespaceId,
	pub database_id: DatabaseId,
	pub table_id: TableId,
	pub name: TableName,
	pub drop: bool,
	pub schemafull: bool,
	pub view: Option<ViewDefinition>,
	pub permissions: Permissions,
	pub table_type: TableType,
	pub changefeed: Option<ChangeFeed>,
	pub comment: Option<String>,
	pub cache_fields_ts: Uuid,
	pub cache_events_ts: Uuid,
	pub cache_tables_ts: Uuid,
	pub cache_indexes_ts: Uuid,
	pub cache_lives_ts: Uuid,
	pub graphql_alias: Option<String>,
	pub graphql_deprecated: Option<String>,
}

/// The SELECT permission guarding a table, for callers holding a table lookup
/// that may have missed.
///
/// A missing definition resolves to [`Permission::None`]: a schemaless table
/// has no recorded grant, and record users must be denied rather than allowed
/// by default. Callers that gate row visibility share this resolution so the
/// absent-table case cannot diverge between them.
pub fn table_select_permission(table_def: Option<&TableDefinition>) -> &Permission {
	match table_def {
		Some(def) => &def.permissions.select,
		None => &Permission::None,
	}
}

impl FromStored for TableDefinition {
	type Stored = StoredTableDefinition;

	fn from_stored(stored: &StoredTableDefinition) -> anyhow::Result<TableDefinition> {
		fn build(stored: &StoredTableDefinition) -> anyhow::Result<TableDefinition> {
			Ok(TableDefinition {
				namespace_id: stored.namespace_id,
				database_id: stored.database_id,
				table_id: stored.table_id,
				name: TableName::from(stored.name.clone()),
				drop: stored.drop,
				schemafull: stored.schemafull,
				view: stored.view.as_ref().map(ViewDefinition::from_stored).transpose()?,
				permissions: Permissions::from_stored(&stored.permissions)?,
				table_type: stored.table_type.clone(),
				changefeed: stored.changefeed,
				comment: stored.comment.clone(),
				cache_fields_ts: stored.cache_fields_ts,
				cache_events_ts: stored.cache_events_ts,
				cache_tables_ts: stored.cache_tables_ts,
				cache_indexes_ts: stored.cache_indexes_ts,
				cache_lives_ts: stored.cache_lives_ts,
				graphql_alias: stored.graphql_alias.clone(),
				graphql_deprecated: stored.graphql_deprecated.clone(),
			})
		}
		build(stored).with_context(|| {
			format!("the stored definition of table `{}` no longer compiles", stored.name)
		})
	}
}

impl TableDefinition {
	/// See [`StoredTableDefinition::allows_normal`]; same predicate on the
	/// compiled form.
	pub fn allows_normal(&self) -> bool {
		matches!(self.table_type, TableType::Normal | TableType::Any)
	}

	/// See [`StoredTableDefinition::allows_relation`]; same predicate on the
	/// compiled form.
	pub fn allows_relation(&self) -> bool {
		matches!(self.table_type, TableType::Relation(_) | TableType::Any)
	}

	pub fn to_stored(&self) -> StoredTableDefinition {
		StoredTableDefinition {
			namespace_id: self.namespace_id,
			database_id: self.database_id,
			table_id: self.table_id,
			name: self.name.clone().into(),
			drop: self.drop,
			schemafull: self.schemafull,
			view: self.view.as_ref().map(ViewDefinition::to_stored),
			permissions: crate::catalog::StoredPermissions::from_runtime(&self.permissions),
			changefeed: self.changefeed,
			comment: self.comment.clone(),
			table_type: self.table_type.clone(),
			cache_fields_ts: self.cache_fields_ts,
			cache_events_ts: self.cache_events_ts,
			cache_tables_ts: self.cache_tables_ts,
			cache_indexes_ts: self.cache_indexes_ts,
			cache_lives_ts: self.cache_lives_ts,
			graphql_alias: self.graphql_alias.clone(),
			graphql_deprecated: self.graphql_deprecated.clone(),
		}
	}
}

impl From<StoredTableDefinition> for crate::val::Value {
	#[inline]
	fn from(v: StoredTableDefinition) -> Self {
		let mut h = std::collections::HashMap::<&str, crate::val::Value>::new();
		h.insert("id", crate::val::Value::Number(crate::val::Number::Int(v.table_id.0 as i64)));
		h.insert("name", crate::val::Value::String(v.name));
		crate::val::Value::Object(crate::val::Object::from(h))
	}
}
