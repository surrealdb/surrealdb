use anyhow::Context as _;
use revision::{
	DeserializeRevisioned, Revisioned, SerializeRevisioned, SkipRevisioned, revisioned,
};
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};
use uuid::Uuid;

use crate::catalog::{
	DatabaseId, FromStored, LogicalPermissionBody, NamespaceId, Permission, Permissions,
	StoredPermissions, StoredViewDefinition, ViewDefinition,
};
use crate::expr::ChangeFeed;
use crate::expr::logical::LogicalPermission;
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

#[revisioned(revision = 4)]
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

	/// Whether packed adjacency blocks exist — or have ever existed — for
	/// this table's records as graph vertices.
	///
	/// Set transactionally with the first block the fold task writes under
	/// the table's `^` subspace, and never cleared: once set, an edge
	/// deletion on one of this table's vertices must write a tombstone
	/// rather than deleting its pointer key (the entry may live in a
	/// block), readers must merge blocks with the pointer keys, and the
	/// edge-existence peeks must probe the block subspace as well. While
	/// unset, none of that machinery engages and the table behaves — byte
	/// for byte — as it did before blocks existed.
	///
	/// Internal bookkeeping: not part of the DEFINE TABLE surface, so it
	/// is excluded from the SQL rendering; surfaced in `INFO` structure
	/// output for observability.
	#[revision(start = 4)]
	pub graph_folded: bool,

	/// Whether this table's shared doc-ID space is referenced from packed
	/// adjacency blocks — its records appear, as edges or as targets, in
	/// numeric-encoded block entries somewhere in the database.
	///
	/// Set transactionally with the first numeric block that compresses one
	/// of this table's record ids, and never cleared: while set, record
	/// deletion must remove the record's doc-ID mapping even when no index
	/// consumes the space (a stale mapping would hand a re-created record
	/// its predecessor's id), and removing the table's last doc-ID-consuming
	/// index must not reclaim the space (block entries still resolve through
	/// it).
	///
	/// Internal bookkeeping: excluded from the SQL rendering; surfaced in
	/// `INFO` structure output for observability.
	#[revision(start = 4)]
	/// `REMOVE TABLE` deletes the table's mapping space while foreign
	/// tables' numeric blocks may still name its ids; recreating the same
	/// table name then restarts allocation, so stale folded entries can
	/// alias new records until the referencing scopes re-fold. Numeric
	/// encoding therefore stays behind its default-off knob until removal
	/// epochs land.
	pub graph_doc_ids: bool,

	/// Cap on the inline adjacency cache kept beside each of this table's
	/// records (the `INLINE EDGES` clause). `None` disables the cache —
	/// there is deliberately no global default, so the caches a reader may
	/// trust exist only where a durable, per-table declaration maintains
	/// them. Changing the stored value invalidates the table's cache
	/// subspace wholesale; caches re-materialise lazily on the write path.
	#[revision(start = 4)]
	pub inline_edges_cap: Option<u32>,

	/// Cap on the inline reference cache (the `INLINE REFERENCES` clause).
	/// Same semantics as `inline_edges_cap`, for reference back-links.
	#[revision(start = 4)]
	pub inline_refs_cap: Option<u32>,

	/// Generation counter of the table's inline-field set. Bumped by every
	/// schema mutation that changes which edge-table fields are inlined
	/// (or an inlined field's type), it stamps every written inline payload
	/// so readers can tell a current payload from one written under an
	/// older field set — stale payloads fall back to the record, never
	/// evaluate silently. Zero means the table has never had an inline
	/// field. Internal bookkeeping: excluded from INFO and export.
	#[revision(start = 4)]
	pub graph_inline_gen: u32,
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
			graph_folded: false,
			graph_doc_ids: false,
			inline_edges_cap: None,
			inline_refs_cap: None,
			graph_inline_gen: 0,
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
			inline_edges_cap: self.inline_edges_cap,
			inline_refs_cap: self.inline_refs_cap,
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
			"graph_folded", if self.graph_folded => true.into(),
			"graph_doc_ids", if self.graph_doc_ids => true.into(),
			"inline_edges_cap", if let Some(v) = self.inline_edges_cap => (v as i64).into(),
			"inline_refs_cap", if let Some(v) = self.inline_refs_cap => (v as i64).into(),
			"id" => self.table_id.0.into(),
		})
	}
}

/// Runtime form of [`StoredTableDefinition`].
///
/// The `PERMISSIONS FOR select` clause is carried twice: as the [`Permission`]
/// the legacy engine evaluates, inside `permissions`, and as a lazily compiled
/// logical body the streaming engine's analyzer reads. Both come from the
/// stored text, so `select_body` names the same clause as `permissions.select`.
/// A constructor that sets one half must set the other from the same source:
/// [`LogicalPermissionBody::of`] does so from the expression a `DEFINE` or
/// `ALTER` statement produced. The body is not part of the stored form.
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
	/// The `PERMISSIONS FOR select` clause in logical form; see
	/// [`Self::select_permission_logical`].
	pub select_body: LogicalPermissionBody,
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
	/// See [`StoredTableDefinition::graph_folded`].
	pub graph_folded: bool,
	/// See [`StoredTableDefinition::graph_doc_ids`].
	pub graph_doc_ids: bool,
	/// See [`StoredTableDefinition::inline_edges_cap`].
	pub inline_edges_cap: Option<u32>,
	/// See [`StoredTableDefinition::inline_refs_cap`].
	pub inline_refs_cap: Option<u32>,
	/// See [`StoredTableDefinition::graph_inline_gen`].
	pub graph_inline_gen: u32,
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
				select_body: LogicalPermissionBody::from_stored(&stored.permissions.select),
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
				graph_folded: stored.graph_folded,
				graph_doc_ids: stored.graph_doc_ids,
				inline_edges_cap: stored.inline_edges_cap,
				inline_refs_cap: stored.inline_refs_cap,
				graph_inline_gen: stored.graph_inline_gen,
			})
		}
		build(stored).with_context(|| {
			format!("the stored definition of table `{}` no longer compiles", stored.name)
		})
	}
}

impl TableDefinition {
	/// The `PERMISSIONS FOR select` clause in logical form, compiling its
	/// guard on first call and keeping it for the life of the definition, so
	/// a statement binding this table does not re-lower the clause.
	///
	/// Errs when the clause has no logical form; the failure is remembered
	/// and repeated calls answer the same.
	pub fn select_permission_logical(&self) -> anyhow::Result<LogicalPermission> {
		self.select_body.logical()
	}

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
			graph_folded: self.graph_folded,
			graph_doc_ids: self.graph_doc_ids,
			inline_edges_cap: self.inline_edges_cap,
			inline_refs_cap: self.inline_refs_cap,
			graph_inline_gen: self.graph_inline_gen,
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

#[cfg(test)]
mod tests {
	use surrealdb_types::ToSql;

	use super::*;
	use crate::catalog::ExprText;
	use crate::expr::Expr;

	fn definition(select: Permission) -> TableDefinition {
		TableDefinition {
			namespace_id: NamespaceId(1),
			database_id: DatabaseId(1),
			table_id: TableId(1),
			name: TableName::from("t"),
			drop: false,
			schemafull: false,
			view: None,
			select_body: LogicalPermissionBody::of(&select),
			permissions: Permissions {
				select,
				..Default::default()
			},
			table_type: TableType::Any,
			changefeed: None,
			comment: None,
			cache_fields_ts: Uuid::nil(),
			cache_events_ts: Uuid::nil(),
			cache_tables_ts: Uuid::nil(),
			cache_indexes_ts: Uuid::nil(),
			cache_lives_ts: Uuid::nil(),
			graphql_alias: None,
			graphql_deprecated: None,
			graph_folded: false,
			graph_doc_ids: false,
			inline_edges_cap: None,
			inline_refs_cap: None,
			graph_inline_gen: 0,
		}
	}

	/// The select body is the stored text compiled for the streaming engine: a
	/// definition built from a statement carries it, a definition read back
	/// from storage carries it, and both compile to the same guard. The
	/// compiled guard is kept on the definition, so the second read returns the
	/// value the first one produced rather than compiling the text again.
	#[test]
	fn the_select_body_compiles_once_per_definition() {
		let guard: Expr = ExprText::from_raw("published = true").compile().unwrap();
		let def = definition(Permission::Specific(guard));
		let loaded = TableDefinition::from_stored(&def.to_stored()).unwrap();
		assert_eq!(loaded, def);

		let LogicalPermissionBody::Specific(body) = &loaded.select_body else {
			panic!("a conditional permission has a body");
		};
		let first = body.logical().unwrap();
		let second = body.logical().unwrap();
		assert!(std::ptr::eq(first, second), "the second read recompiled the body");
		assert_eq!(first.to_sql(), "published = true");
		assert_eq!(
			loaded.select_permission_logical().unwrap(),
			LogicalPermission::Specific(first.clone())
		);
	}

	/// The unconditional permissions carry no text to compile, and the body
	/// still names the same clause as the legacy half.
	#[test]
	fn unconditional_permissions_have_no_body_to_compile() {
		let full = TableDefinition::from_stored(&definition(Permission::Full).to_stored()).unwrap();
		assert_eq!(full.select_permission_logical().unwrap(), LogicalPermission::Full);
		let none = TableDefinition::from_stored(&definition(Permission::None).to_stored()).unwrap();
		assert_eq!(none.select_permission_logical().unwrap(), LogicalPermission::None);
	}

	/// A guard the lowering cannot express compiles as the legacy `Expr` and
	/// fails as a logical body, and the failure is remembered rather than
	/// retried.
	#[test]
	fn a_guard_with_no_logical_form_fails_to_compile_and_stays_failed() {
		let guard: Expr = ExprText::from_raw("id IN |t:1..3|").compile().unwrap();
		let def = definition(Permission::Specific(guard));
		let loaded = TableDefinition::from_stored(&def.to_stored()).unwrap();
		assert_eq!(loaded.permissions, def.permissions);
		let first = loaded.select_permission_logical().unwrap_err().to_string();
		let second = loaded.select_permission_logical().unwrap_err().to_string();
		assert!(first.contains("no logical form"), "{first}");
		assert_eq!(first, second);
	}
}
