use std::sync::Arc;

use anyhow::Context as _;
use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use super::{
	KeyStructure, KeyStructureError, LogicalPermissionBody, StoredPermission, StoredReference,
};
use crate::catalog::auth::AuthLimit;
use crate::catalog::{
	ExprText, FromStored, IdiomText, KindText, LogicalBody, Permission, reference_to_stored,
};
use crate::expr::logical::{LogicalExpr, LogicalPermission};
use crate::expr::reference::Reference as ExprReference;
use crate::expr::statements::define::{SqlType, SqlTypeDialect};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Idiom, KeyNames, Kind};
use crate::stored::Stored;
use crate::val::{TableName, Value};
use crate::{expr as runtime, impl_kv_value_catalog};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub enum StoredDefineDefault {
	#[default]
	#[stored(n = 1)]
	None,
	/// Canonical SurrealQL text of the default-value expression.
	#[stored(n = 2)]
	Always(ExprText),
	#[stored(n = 3)]
	Set(ExprText),
}

impl StoredDefineDefault {
	/// The clause's stored expression text, or `None` for a field with no
	/// `DEFAULT`. `DEFAULT` and `DEFAULT ALWAYS` differ in when the clause is
	/// applied, not in what it holds.
	pub fn expression(&self) -> Option<&ExprText> {
		match self {
			StoredDefineDefault::None => None,
			StoredDefineDefault::Always(text) | StoredDefineDefault::Set(text) => Some(text),
		}
	}
}

/// Dependency metadata for a computed field.
///
/// Tracks which same-table fields a computed expression references, and whether
/// the static analysis was able to fully determine all dependencies.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct ComputedDeps {
	/// Known same-table field names this computed field depends on.
	pub fields: Vec<String>,
	/// Whether static analysis could fully determine all dependencies.
	///
	/// When `false`, the expression contains opaque constructs (subqueries, params,
	/// graph traversals, etc.) that could access arbitrary fields at runtime.
	/// If such a field is needed by a query, ALL computed fields must be evaluated.
	pub is_complete: bool,
}

/// The SQL dialect of a stored `SQL TYPE` clause.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub enum StoredSqlTypeDialect {
	#[stored(n = 1)]
	PostgreSql,
	#[stored(n = 2)]
	MySql,
	#[stored(n = 3)]
	DynamoDb,
}

/// A stored `SQL TYPE` clause: the dialect and the type text as written.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredSqlType {
	#[stored(n = 2)]
	pub dialect: StoredSqlTypeDialect,
	#[stored(n = 4)]
	pub text: String,
}

#[revisioned(revision = 7)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredFieldDefinition {
	/// Canonical SurrealQL text of the field's path.
	///
	/// TODO: Needs to be its own grammar rather than a whole idiom. A field
	/// path may only be a chain of name/`*`/destructure parts; the parser
	/// this text compiles through (`syn::idiom_for_definition`) still admits
	/// shapes a field path has no meaning for, such as a `[WHERE ...]` filter
	/// or a computed `[$expr]` index, so the restriction is enforced by the
	/// `DEFINE FIELD` parser rather than by the type.
	#[stored(n = 2)]
	pub name: IdiomText,
	#[stored(n = 4)]
	pub table: TableName,
	/// Canonical SurrealQL kind-grammar text of the `TYPE` clause.
	#[revision(end = 5, convert_fn = "convert_field_kind")]
	pub old_field_kind: Option<Kind>,
	#[revision(start = 5)]
	#[stored(n = 6)]
	pub field_kind: Option<KindText>,
	#[stored(n = 8)]
	pub flexible: bool,
	#[stored(n = 10)]
	pub readonly: bool,
	/// Canonical SurrealQL text of the `VALUE` clause's expression.
	#[stored(n = 12)]
	pub value: Option<ExprText>,
	/// Canonical SurrealQL text of the `ASSERT` clause's expression.
	#[stored(n = 14)]
	pub assert: Option<ExprText>,
	/// Canonical SurrealQL text of the `COMPUTED` clause's expression.
	#[stored(n = 16)]
	pub computed: Option<ExprText>,
	#[stored(n = 18)]
	pub default: StoredDefineDefault,

	#[stored(n = 20)]
	pub select_permission: StoredPermission,
	#[stored(n = 22)]
	pub create_permission: StoredPermission,
	#[stored(n = 24)]
	pub update_permission: StoredPermission,

	#[stored(
		n = 1,
		ignorable,
		independent = "a comment is text for people and no other field reads it"
	)]
	pub comment: Option<String>,
	#[stored(n = 26)]
	pub reference: Option<StoredReference>,

	/// The auth limit of the API.
	#[revision(start = 2, default_fn = "default_auth_limit")]
	#[stored(n = 28)]
	pub auth_limit: AuthLimit,

	/// Pre-computed dependency metadata for computed fields. Removed at
	/// revision 5: deps are always extracted on-the-fly now (see
	/// `crate::expr::computed_deps::extract_computed_deps`), matching the
	/// catalog's "stored = user intent, derived = recomputed" split that
	/// motivated the `field_kind`/`value`/`assert`/`computed` text swaps.
	#[revision(start = 3, end = 5, convert_fn = "convert_computed_deps")]
	pub computed_deps: Option<ComputedDeps>,

	/// Optional alias used as the GraphQL field name. When set, GraphQL
	/// schema generation prefers this over the raw SurrealQL field name,
	/// allowing snake_case columns to be exposed as camelCase. See
	/// GitHub issue #4537. `Option<String>::default()` already returns
	/// `None` so no explicit `default_fn` is needed.
	#[revision(start = 4)]
	#[stored(n = 30)]
	pub graphql_alias: Option<String>,

	/// Reason emitted on the GraphQL `@deprecated` directive. When set, the
	/// corresponding GraphQL field is marked deprecated in introspection
	/// (and in input objects), surfacing the reason to schema consumers
	/// while remaining usable for backwards compatibility.
	#[revision(start = 4)]
	#[stored(n = 32)]
	pub graphql_deprecated: Option<String>,

	/// Whether this field's value is inlined into the edge's vertex-side
	/// adjacency values (the `INLINE` clause), so an edge-table predicate
	/// over it can be evaluated during a traversal without fetching the
	/// edge record. Only top-level, non-computed fields of a non-lightweight
	/// relation may set it.
	#[revision(start = 6)]
	#[stored(n = 34)]
	pub inline: bool,

	/// The declared SQL type, the `SQL TYPE` clause. Every write to the field, SurrealQL
	/// included, is checked and canonicalised against it (length, precision and scale,
	/// padding), and the SQL front ends report it as the column type. The field is
	/// ignorable by design: a release that skips it keeps the field's kind and still
	/// accepts writes, but no longer refuses an overlong `varchar(n)`, rounds a
	/// `numeric(p,s)`, or pads a `char(n)` / `BINARY(n)` value.
	#[revision(start = 7)]
	#[stored(
		n = 3,
		ignorable,
		independent = "a release without the clause keeps the field kind and its writes; it only stops enforcing and canonicalising the declared SQL type"
	)]
	pub sql_type: Option<StoredSqlType>,
}

impl StoredFieldDefinition {
	// This was pushed in after the first beta, so we need to add auth_limit to structs in a
	// non-breaking way
	fn default_auth_limit(_revision: u16) -> Result<AuthLimit, revision::Error> {
		Ok(AuthLimit::new_no_limit())
	}

	/// Discards the old structured dependency metadata: revision 5+ always
	/// re-extracts deps on demand instead of reading a stored cache.
	fn convert_computed_deps(
		&mut self,
		_rev: u16,
		_value: Option<ComputedDeps>,
	) -> Result<(), revision::Error> {
		Ok(())
	}

	/// Renders the old structured `Kind` to canonical SurrealQL kind-grammar
	/// text, matching what `Kind::to_sql()` already produces (and what the
	/// `DEFINE FIELD` parser feeds into this field going forward).
	fn convert_field_kind(
		&mut self,
		_rev: u16,
		value: Option<Kind>,
	) -> Result<(), revision::Error> {
		self.field_kind = value.map(|k| KindText::new(&k));
		Ok(())
	}
}
impl_kv_value_catalog!(StoredFieldDefinition);

impl InfoStructure for FieldDefinition {
	fn structure(self) -> Value {
		let key = self.live_key().cloned();
		Value::from(map! {
			"name" => self.name.structure(),
			"table" => Value::String(self.table.into()),
			"kind", if let Some(v) = self.field_kind => Value::from(v.to_sql()),
			"components", if let Some(v) = &key => v.components_structure(),
			"flexible", if self.flexible => true.into(),
			"value", if let Some(v) = self.value => Value::from(v.to_stored_sql()),
			"assert", if let Some(v) = self.assert => Value::from(v.to_stored_sql()),
			"computed", if let Some(v) = self.computed => Value::from(v.to_stored_sql()),
			"default_always", if matches!(&self.default, DefineDefault::Always(_) | DefineDefault::Set(_)) => Value::Bool(matches!(self.default, DefineDefault::Always(_))), // Only reported if DEFAULT is also enabled for this field
			"default", if let DefineDefault::Always(v) | DefineDefault::Set(v) = self.default => Value::from(v.to_stored_sql()),
			"reference", if let Some(v) = self.reference => v.structure(),
			"readonly" => self.readonly.into(),
			"inline", if self.inline => true.into(),
			"permissions" => Value::from(map!{
				"select" => self.select_permission.structure(),
				"create" => self.create_permission.structure(),
				"update" => self.update_permission.structure(),
			}),
			"comment", if let Some(v) = self.comment => v.into(),
			"graphql_alias", if let Some(v) = self.graphql_alias => v.into(),
			"graphql_deprecated", if let Some(v) = self.graphql_deprecated => v.into(),
			"sql_type", if let Some(v) = self.sql_type => Value::from(map!{
				"dialect" => Value::String(v.dialect.keyword().into()),
				"text" => Value::String(v.text.into()),
			}),
		})
	}
}

impl FieldDefinition {
	/// Builds the runtime statement rendered by both INFO and schema export.
	/// Its clauses use the same canonical printer as parsed definitions.
	pub fn to_runtime_definition(
		&self,
		kind: runtime::statements::define::DefineKind,
	) -> runtime::statements::define::DefineFieldStatement {
		runtime::statements::define::DefineFieldStatement {
			kind,
			name: crate::expr::Expr::Idiom(self.name.clone()),
			what: runtime::Expr::Table(self.table.clone()),
			field_kind: self.field_kind.clone(),
			key_names: self.live_key().and_then(|key| key.names.clone()),
			flexible: self.flexible,
			readonly: self.readonly,
			inline: self.inline,
			value: self.value.clone(),
			assert: self.assert.clone(),
			computed: self.computed.clone(),
			default: match &self.default {
				DefineDefault::None => runtime::statements::define::DefineDefault::None,
				DefineDefault::Set(x) => runtime::statements::define::DefineDefault::Set(x.clone()),
				DefineDefault::Always(x) => {
					runtime::statements::define::DefineDefault::Always(x.clone())
				}
			},
			permissions: runtime::permission::Permissions {
				select: self.select_permission.clone(),
				create: self.create_permission.clone(),
				update: self.update_permission.clone(),
				delete: runtime::permission::Permission::Full,
			},
			comment: self
				.comment
				.clone()
				.map(|x| runtime::Expr::Literal(runtime::Literal::String(x.into())))
				.unwrap_or(runtime::Expr::Literal(runtime::Literal::None)),
			reference: self.reference.clone(),
			graphql_alias: self.graphql_alias.clone(),
			graphql_deprecated: self.graphql_deprecated.clone(),
			migrate: None,
			sql_type: self.sql_type.clone(),
		}
	}

	/// Renders as `DEFINE FIELD OVERWRITE ...`, for idempotent re-import via
	/// export (relation tables auto-generate in/out fields, and array types
	/// generate sub-field definitions that would conflict on plain re-DEFINE).
	pub fn to_sql_overwrite(&self) -> String {
		self.to_runtime_definition(runtime::statements::define::DefineKind::Overwrite).to_sql()
	}
}

impl ToSql for FieldDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_runtime_definition(runtime::statements::define::DefineKind::Default).fmt_sql(f, fmt)
	}
}

/// Runtime form of [`StoredDefineDefault`]: the default expression parsed.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum DefineDefault {
	#[default]
	None,
	Always(Expr),
	Set(Expr),
}

impl FromStored for DefineDefault {
	type Stored = StoredDefineDefault;

	fn from_stored(stored: &StoredDefineDefault) -> anyhow::Result<DefineDefault> {
		Ok(match stored {
			StoredDefineDefault::None => DefineDefault::None,
			StoredDefineDefault::Always(t) => DefineDefault::Always(t.compile()?),
			StoredDefineDefault::Set(t) => DefineDefault::Set(t.compile()?),
		})
	}
}

impl DefineDefault {
	/// The clause's expression, or `None` for a field with no `DEFAULT`;
	/// [`StoredDefineDefault::expression`] for the stored half.
	pub fn expression(&self) -> Option<&Expr> {
		match self {
			DefineDefault::None => None,
			DefineDefault::Always(expr) | DefineDefault::Set(expr) => Some(expr),
		}
	}

	pub fn to_stored(&self) -> StoredDefineDefault {
		match self {
			DefineDefault::None => StoredDefineDefault::None,
			DefineDefault::Always(e) => StoredDefineDefault::Always(ExprText::new(e)),
			DefineDefault::Set(e) => StoredDefineDefault::Set(ExprText::new(e)),
		}
	}
}

/// Runtime form of [`StoredFieldDefinition`].
///
/// The five clauses the streaming engine evaluates — `COMPUTED`, `DEFAULT`,
/// `VALUE`, `ASSERT` and `PERMISSIONS FOR select` — are carried twice: as the
/// [`Expr`] the legacy engine evaluates and as a lazily compiled logical body.
/// Both come from the stored text, so `computed_body` is `Some` exactly when
/// `computed` is, `value_body` when `value` is, `assert_body` when `assert`
/// is, `default_body` exactly when `default` is not [`DefineDefault::None`],
/// and `select_body` names the same clause as `select_permission`. A
/// constructor that sets one half must set the other from the same source:
/// [`LogicalBody::of`] and [`LogicalPermissionBody::of`] do so from the
/// expression a `DEFINE` statement produced.
///
/// The body is what keeps the cost of a clause per *definition* rather than
/// per record: it is compiled once, on first use, and the definition is shared
/// for as long as the catalog holds it. Reading the [`Expr`] half and building
/// a body from it instead renders the expression back to text and re-parses it
/// for every record the clause is applied to.
///
/// Two fields exist only at runtime and are never written to
/// [`StoredFieldDefinition`]. [`Self::key`] is set on the definition of an `id`
/// whose `TYPE` names its components, and [`Self::key_part`] on the virtual
/// definition of one such component (see [`KeyStructure::derived_fields`]).
/// [`Self::set_type`] is the only writer of `key`, and sets it together with the
/// type. A type written to `field_kind` directly leaves `key` stale, so every
/// reader acts on [`Self::live_key`], which is the structure only while it still
/// describes the type. A type is written through `set_type` so that the two agree.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FieldDefinition {
	pub name: Idiom,
	pub table: TableName,
	pub field_kind: Option<Kind>,
	/// The structure of the record ids of the table, for an `id` definition whose
	/// `TYPE` names its components. Runtime-only; set through [`Self::set_type`] and
	/// read through [`Self::live_key`].
	pub key: Option<Arc<KeyStructure>>,
	/// The position in the record id that this definition reads, for the virtual
	/// definition of a named component. Runtime-only: such a definition is derived
	/// from the `id` definition, never stored, and never written to the catalog.
	pub key_part: Option<u32>,
	pub flexible: bool,
	pub readonly: bool,
	pub inline: bool,
	pub value: Option<Expr>,
	/// The `VALUE` clause in logical form; see [`Self::value_logical`].
	pub value_body: Option<LogicalBody>,
	pub assert: Option<Expr>,
	/// The `ASSERT` clause in logical form; see [`Self::assert_logical`].
	pub assert_body: Option<LogicalBody>,
	pub computed: Option<Expr>,
	/// The `COMPUTED` clause in logical form; see [`Self::computed_logical`].
	pub computed_body: Option<LogicalBody>,
	pub default: DefineDefault,
	/// The `DEFAULT` clause in logical form; see [`Self::default_logical`].
	pub default_body: Option<LogicalBody>,
	pub select_permission: Permission,
	/// The `PERMISSIONS FOR select` clause in logical form; see
	/// [`Self::select_permission_logical`].
	pub select_body: LogicalPermissionBody,
	pub create_permission: Permission,
	pub update_permission: Permission,
	pub reference: Option<ExprReference>,
	pub auth_limit: AuthLimit,
	pub comment: Option<String>,
	pub graphql_alias: Option<String>,
	pub graphql_deprecated: Option<String>,
	/// The declared SQL type, the `SQL TYPE` clause.
	pub sql_type: Option<SqlType>,
}

impl FromStored for FieldDefinition {
	type Stored = StoredFieldDefinition;

	fn from_stored(stored: &StoredFieldDefinition) -> anyhow::Result<FieldDefinition> {
		fn build(stored: &StoredFieldDefinition) -> anyhow::Result<FieldDefinition> {
			let name = stored.name.compile()?;
			// Only the type of `id` can name the components of a record id.
			let (field_kind, names) = match &stored.field_kind {
				Some(text) if name.is_id() => {
					let (kind, names) = text.compile_keyed()?;
					(Some(kind), names)
				}
				Some(text) => (Some(text.compile()?), None),
				None => (None, None),
			};
			let mut definition = FieldDefinition {
				name,
				table: stored.table.clone(),
				field_kind: None,
				key: None,
				key_part: None,
				flexible: stored.flexible,
				readonly: stored.readonly,
				inline: stored.inline,
				value: stored.value.as_ref().map(|t| t.compile()).transpose()?,
				value_body: stored.value.as_ref().map(LogicalBody::new),
				assert: stored.assert.as_ref().map(|t| t.compile()).transpose()?,
				assert_body: stored.assert.as_ref().map(LogicalBody::new),
				computed: stored.computed.as_ref().map(|t| t.compile()).transpose()?,
				computed_body: stored.computed.as_ref().map(LogicalBody::new),
				default: DefineDefault::from_stored(&stored.default)?,
				default_body: stored.default.expression().map(LogicalBody::new),
				select_permission: Permission::from_stored(&stored.select_permission)?,
				select_body: LogicalPermissionBody::from_stored(&stored.select_permission),
				create_permission: Permission::from_stored(&stored.create_permission)?,
				update_permission: Permission::from_stored(&stored.update_permission)?,
				reference: stored.reference.as_ref().map(ExprReference::from_stored).transpose()?,
				auth_limit: stored.auth_limit.clone(),
				comment: stored.comment.clone(),
				graphql_alias: stored.graphql_alias.clone(),
				graphql_deprecated: stored.graphql_deprecated.clone(),
				sql_type: stored.sql_type.as_ref().map(sql_type_runtime),
			};
			definition.set_type(field_kind, names);
			Ok(definition)
		}
		build(stored).with_context(|| {
			format!(
				"the stored definition of field `{}` on table `{}` no longer compiles",
				stored.name.as_str(),
				stored.table
			)
		})
	}
}

impl FieldDefinition {
	/// Sets the type of the field and the names of its record id components,
	/// together, so that [`Self::key`] agrees with [`Self::field_kind`].
	///
	/// `names` belongs to the `id` field and to a tuple type with one element per
	/// name; for anything else the names are dropped, the type is kept, and
	/// [`Self::key`] is `None`. A component whose type a key range cannot reason
	/// about is kept, with an unknown order ([`KeyStructure::from_stored_id_field`]).
	///
	/// Infallible, so that reading the catalog never refuses a table over its
	/// component names. `DEFINE FIELD` and `ALTER FIELD` run the strict check,
	/// [`Self::set_type_strict`], first.
	pub fn set_type(&mut self, kind: Option<Kind>, names: Option<KeyNames>) {
		self.field_kind = kind;
		self.key = None;
		let Some(names) = names else {
			return;
		};
		let structure = match &self.field_kind {
			Some(kind) if self.name.is_id() => KeyStructure::from_stored_id_field(kind, &names),
			_ => None,
		};
		match structure {
			Some(structure) => self.key = Some(Arc::new(structure)),
			None => warn!(
				"the component names of the record id of table `{}` do not fit the type of its `id` field and are ignored",
				self.table
			),
		}
	}

	/// [`Self::set_type`], refusing a type whose components a key range cannot
	/// reason about ([`KeyStructure::from_id_field`]). The definition is unchanged
	/// on error.
	///
	/// # Errors
	///
	/// The first fault in the type, as [`KeyStructure::from_id_field`] reports it;
	/// a type that is not a tuple is [`KeyStructureError::NotATuple`].
	pub fn set_type_strict(
		&mut self,
		kind: Option<Kind>,
		names: Option<KeyNames>,
	) -> Result<(), KeyStructureError> {
		if let Some(names) = &names {
			let kind = kind.as_ref().ok_or(KeyStructureError::NotATuple)?;
			KeyStructure::from_id_field(kind, names)?;
		}
		self.set_type(kind, names);
		Ok(())
	}

	/// The key structure, when it still describes [`Self::field_kind`]. The
	/// structure is derived from the type, so a type written to `field_kind`
	/// directly leaves it stale; the type is what the definition declares.
	///
	/// A reader that acts on the structure (the table's key, the derived component
	/// fields, a write, a planner rule) reads it through this method rather than
	/// from [`Self::key`], so that a stale structure is never acted on. It is also
	/// what a definition renders and stores.
	pub fn live_key(&self) -> Option<&Arc<KeyStructure>> {
		let key = self.key.as_ref()?;
		self.field_kind.as_ref().is_some_and(|kind| key.declares(kind)).then_some(key)
	}

	/// Whether this is the virtual definition of a named record id component.
	/// Such a definition is derived at runtime and is not a definition anyone
	/// wrote; see [`KeyStructure::derived_fields`].
	pub fn is_key_part(&self) -> bool {
		self.key_part.is_some()
	}
}

impl FieldDefinition {
	/// The `COMPUTED` clause as the logical scalar the streaming engine
	/// evaluates, compiled on first call; `None` for a stored field.
	///
	/// Errs when the clause has no logical form, which a `COMPUTED` body
	/// cannot have unless the stored text is a write statement; the analyzer
	/// then declines the table rather than evaluating the legacy form.
	pub fn computed_logical(&self) -> Option<anyhow::Result<&LogicalExpr>> {
		self.computed_body.as_ref().map(LogicalBody::logical)
	}

	/// The `DEFAULT` clause as the logical scalar the streaming engine
	/// evaluates, compiled on first call; `None` for a field with no
	/// `DEFAULT`.
	///
	/// One accessor for both spellings: `DEFAULT` and `DEFAULT ALWAYS` differ
	/// in when the clause is applied, not in what it evaluates to, and
	/// [`Self::default`] is what says which was written.
	///
	/// Errs when the clause has no logical form — a write statement, a mock
	/// record set in scalar position — and the caller then evaluates the
	/// [`Expr`] half instead.
	pub fn default_logical(&self) -> Option<anyhow::Result<&LogicalExpr>> {
		self.default_body.as_ref().map(LogicalBody::logical)
	}

	/// The `VALUE` clause as the logical scalar the streaming engine
	/// evaluates, compiled on first call; `None` for a field with no `VALUE`.
	///
	/// Errs when the clause has no logical form — a write statement, a mock
	/// record set in scalar position — and the caller then evaluates the
	/// [`Expr`] half instead.
	pub fn value_logical(&self) -> Option<anyhow::Result<&LogicalExpr>> {
		self.value_body.as_ref().map(LogicalBody::logical)
	}

	/// The `ASSERT` clause as the logical scalar the streaming engine
	/// evaluates, compiled on first call; `None` for a field with no `ASSERT`.
	///
	/// Errs on the same terms as [`Self::value_logical`].
	pub fn assert_logical(&self) -> Option<anyhow::Result<&LogicalExpr>> {
		self.assert_body.as_ref().map(LogicalBody::logical)
	}

	/// The `PERMISSIONS FOR select` clause in logical form, compiling its
	/// guard on first call.
	pub fn select_permission_logical(&self) -> anyhow::Result<LogicalPermission> {
		self.select_body.logical()
	}

	/// The same-table fields this field needs a value from before its own value
	/// can be produced.
	///
	/// Only the clauses that *produce* a value contribute: `DEFAULT`, `VALUE`
	/// and `COMPUTED`. `ASSERT` is excluded because it validates a value that
	/// has already been produced rather than producing one, so it imposes no
	/// order — and two fields asserting against each other is ordinary, not a
	/// cycle:
	///
	/// ```surql
	/// DEFINE FIELD in  ON follows TYPE record<user> ASSERT in != out;
	/// DEFINE FIELD out ON follows TYPE record<user> ASSERT out != in;
	/// ```
	///
	/// Callers use this to order field evaluation so a clause runs after the
	/// fields it reads, and to reject a schema whose producing clauses read each
	/// other in a cycle.
	///
	/// A `DEFAULT` / `VALUE` clause naming its own field is reading that field's
	/// current value, not asking for one to be produced first, so it is not a
	/// dependency: `DEFINE FIELD n ON t VALUE n ?? 0` is ordinary. A `COMPUTED`
	/// clause naming its own field has nothing else to derive from and is
	/// reported as a cycle.
	///
	/// A clause containing an opaque construct (a subquery, a parameter, a
	/// graph traversal) contributes only the dependencies static analysis could
	/// determine, so an ordering derived from this is best-effort for those.
	pub fn production_dependencies(&self) -> Vec<String> {
		let mut deps = Vec::new();
		if let Some(expr) = &self.value {
			deps.extend(self.reads_of(expr));
		}
		match &self.default {
			DefineDefault::Set(expr) | DefineDefault::Always(expr) => {
				deps.extend(self.reads_of(expr))
			}
			DefineDefault::None => {}
		}
		// A COMPUTED clause keeps its self-reference: it has nothing else to
		// derive from, so naming itself is a cycle rather than a read
		if let Some(expr) = &self.computed {
			deps.extend(crate::expr::computed_deps::extract_computed_deps(expr).fields);
		}
		deps.sort();
		deps.dedup();
		deps
	}

	/// The same-table fields one clause of this field reads, excluding the field
	/// itself — naming your own field is reading the value under consideration,
	/// not asking for one to be produced first.
	fn reads_of(&self, expr: &Expr) -> Vec<String> {
		// Dependencies are record keys, so the field is named by its own: a
		// nested field has none and nothing reads it by name.
		let own_name = self.name.field_name();
		crate::expr::computed_deps::extract_computed_deps(expr)
			.fields
			.into_iter()
			.filter(|dep| Some(dep.as_str()) != own_name)
			.collect()
	}

	/// The same-table fields this field's `ASSERT` clause reads.
	///
	/// Kept apart from [`Self::production_dependencies`] because these impose no
	/// evaluation order: an `ASSERT` runs after every field has been produced,
	/// so it reads final values whatever order production happened in. Callers
	/// need them only to know which `COMPUTED` fields have to be materialised
	/// for the validation pass to read.
	///
	/// A clause naming its own field is excluded for the same reason as in
	/// `production_dependencies`: it is reading the value being validated.
	pub fn assert_dependencies(&self) -> Vec<String> {
		let Some(expr) = &self.assert else {
			return Vec::new();
		};
		let mut deps = self.reads_of(expr);
		deps.sort();
		deps.dedup();
		deps
	}

	/// Whether this field has a clause that produces a value per record, and so
	/// can take part in a dependency cycle with another field.
	/// See [`Self::production_dependencies`].
	pub fn has_production_clause(&self) -> bool {
		self.value.is_some()
			|| self.computed.is_some()
			|| !matches!(self.default, DefineDefault::None)
	}

	/// The name a definition of field `name` is stored under: the `fd`
	/// segment of its key, by which every write, point read and removal of the
	/// definition addresses it.
	///
	/// It is derived from the path alone, so every spelling of one path names
	/// one key: the raw rendering ([`Idiom::to_raw_string`]), which quotes a
	/// part only where it would not lex as an identifier and leaves a reserved
	/// keyword bare — `select.*` for `` `select`.* ``.
	pub fn storage_name(name: &Idiom) -> String {
		name.to_raw_string()
	}

	/// The stored form of this definition. A definition whose `id` names its
	/// components stores the names in the text of its type
	/// ([`KindText::new_keyed`]); no other runtime state is stored.
	///
	/// A virtual definition ([`Self::key_part`]) has no stored form and is never
	/// passed here.
	pub fn to_stored(&self) -> StoredFieldDefinition {
		debug_assert!(self.key_part.is_none(), "a derived field definition has no stored form");
		StoredFieldDefinition {
			name: IdiomText::new(&self.name),
			table: self.table.clone(),
			field_kind: match self.live_key() {
				Some(key) => Some(KindText::new_keyed(key)),
				None => self.field_kind.as_ref().map(KindText::new),
			},
			flexible: self.flexible,
			readonly: self.readonly,
			inline: self.inline,
			value: self.value.as_ref().map(ExprText::new),
			assert: self.assert.as_ref().map(ExprText::new),
			computed: self.computed.as_ref().map(ExprText::new),
			default: self.default.to_stored(),
			select_permission: crate::catalog::StoredPermission::from_runtime(
				&self.select_permission,
			),
			create_permission: crate::catalog::StoredPermission::from_runtime(
				&self.create_permission,
			),
			update_permission: crate::catalog::StoredPermission::from_runtime(
				&self.update_permission,
			),
			comment: self.comment.clone(),
			reference: self.reference.as_ref().map(reference_to_stored),
			auth_limit: self.auth_limit.clone(),
			graphql_alias: self.graphql_alias.clone(),
			graphql_deprecated: self.graphql_deprecated.clone(),
			sql_type: self.sql_type.as_ref().map(sql_type_to_stored),
		}
	}
}

/// The stored form of a declared SQL type.
fn sql_type_to_stored(sql_type: &SqlType) -> StoredSqlType {
	StoredSqlType {
		dialect: match sql_type.dialect {
			SqlTypeDialect::PostgreSql => StoredSqlTypeDialect::PostgreSql,
			SqlTypeDialect::MySql => StoredSqlTypeDialect::MySql,
			SqlTypeDialect::DynamoDb => StoredSqlTypeDialect::DynamoDb,
		},
		text: sql_type.text.clone(),
	}
}

/// The runtime form of a stored declared SQL type.
fn sql_type_runtime(stored: &StoredSqlType) -> SqlType {
	SqlType {
		dialect: match stored.dialect {
			StoredSqlTypeDialect::PostgreSql => SqlTypeDialect::PostgreSql,
			StoredSqlTypeDialect::MySql => SqlTypeDialect::MySql,
			StoredSqlTypeDialect::DynamoDb => SqlTypeDialect::DynamoDb,
		},
		text: stored.text.clone(),
	}
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use super::*;

	/// The stored field path is the idiom's canonical raw rendering — the
	/// same text `Idiom`'s own wire format has always carried, so the encoded
	/// bytes are the bare string either way — and `from_stored` must invert
	/// it exactly, including for paths that need escaping or that carry
	/// non-field parts.
	#[test]
	fn field_name_round_trips_through_stored_text() {
		for raw in ["name", "a.b", "settings.theme", "tags[0]", "`a.b`", "`select`"] {
			let name = Idiom::from_str(raw).unwrap();
			let def = FieldDefinition {
				name: name.clone(),
				table: TableName::from("t"),
				..Default::default()
			};

			let stored = def.to_stored();
			assert_eq!(
				stored.name.as_str(),
				name.to_raw_string(),
				"stored text for `{raw}` is not the idiom's raw rendering"
			);
			assert_eq!(
				FieldDefinition::from_stored(&stored).unwrap(),
				def,
				"`{raw}` did not survive to_stored/from_stored"
			);
		}
	}

	/// A clause reading its own field is reading the value under consideration,
	/// not asking for one to be produced first, and a read of another field is
	/// a dependency named by the key a record holds it under, whatever either
	/// name has to be backticked as when written.
	#[test]
	fn dependencies_are_named_by_the_unescaped_field_name() {
		for name in ["plain", "MixedCase", "the x", "a-b", "café", "1st", "a.b"] {
			let escaped = format!("`{name}`");
			let value = |text: &str| -> FieldDefinition {
				FieldDefinition {
					name: Idiom::from_str(&escaped).unwrap(),
					table: TableName::from("t"),
					value: Some(ExprText::from_raw(text).compile().unwrap()),
					..Default::default()
				}
			};

			let own = value(&format!("{escaped} ?? 'init'"));
			assert!(
				own.production_dependencies().is_empty(),
				"`{name}` reading itself is not a dependency: {:?}",
				own.production_dependencies()
			);

			let other = value("`another one` + 1");
			assert_eq!(other.production_dependencies(), ["another one"]);
			assert!(other.assert_dependencies().is_empty());

			let reads_own_name = FieldDefinition {
				name: Idiom::from_str("`another one`").unwrap(),
				table: TableName::from("t"),
				value: Some(ExprText::from_raw(format!("{escaped} + 1")).compile().unwrap()),
				..Default::default()
			};
			assert_eq!(reads_own_name.production_dependencies(), [name]);
		}
	}

	/// A clause reading its own field is not a dependency, whether or not the
	/// field's name needs quoting: the read records the bare identifier, and
	/// the field is recognised by that same name.
	#[test]
	fn a_clause_reading_its_own_field_is_not_a_dependency() {
		for raw in ["n", "`n-x`", "`verrouillé`"] {
			let name = Idiom::from_str(raw).unwrap();
			let def = FieldDefinition {
				name: name.clone(),
				table: TableName::from("t"),
				value: Some(Expr::Idiom(name.clone())),
				assert: Some(Expr::Idiom(name)),
				..Default::default()
			};
			assert!(
				def.production_dependencies().is_empty(),
				"`{raw}` reads itself as a dependency: {:?}",
				def.production_dependencies()
			);
			assert!(
				def.assert_dependencies().is_empty(),
				"`{raw}` asserts against itself as a dependency: {:?}",
				def.assert_dependencies()
			);
		}
	}

	/// A nested field reading the top-level field whose quoted name spells its
	/// path reads another field, not itself.
	#[test]
	fn a_nested_field_reading_a_quoted_field_spelling_its_path_has_a_dependency() {
		let def = FieldDefinition {
			name: Idiom::from_str("is.locked").unwrap(),
			table: TableName::from("t"),
			value: Some(Expr::Idiom(Idiom::from_str("`is.locked`").unwrap())),
			assert: Some(Expr::Idiom(Idiom::from_str("`is.locked`").unwrap())),
			..Default::default()
		};
		assert_eq!(def.production_dependencies(), vec!["is.locked".to_owned()]);
		assert_eq!(def.assert_dependencies(), vec!["is.locked".to_owned()]);
	}

	/// The logical bodies are the stored text compiled for the streaming
	/// engine: a definition built from a statement carries them, a definition
	/// read back from storage carries them, and both compile to the same
	/// scalar. The legacy `Expr` half never feeds the logical half.
	#[test]
	fn computed_and_select_clauses_compile_to_their_logical_form() {
		let computed: Expr = ExprText::from_raw("price * qty").compile().unwrap();
		let guard: Expr = ExprText::from_raw("$auth.id = owner").compile().unwrap();
		let def = FieldDefinition {
			name: Idiom::field("total"),
			table: TableName::from("t"),
			computed: Some(computed.clone()),
			computed_body: Some(LogicalBody::of(&computed)),
			select_permission: Permission::Specific(guard.clone()),
			select_body: LogicalPermissionBody::of(&Permission::Specific(guard)),
			..Default::default()
		};
		let stored = def.to_stored();
		let loaded = FieldDefinition::from_stored(&stored).unwrap();
		assert_eq!(loaded, def);

		let body = loaded.computed_logical().expect("a computed field").unwrap();
		assert_eq!(surrealdb_types::ToSql::to_sql(body), "price * qty");
		let LogicalPermission::Specific(guard) = loaded.select_permission_logical().unwrap() else {
			panic!("expected a conditional permission");
		};
		assert_eq!(surrealdb_types::ToSql::to_sql(&guard), "$auth.id = owner");

		let plain = FieldDefinition::from_stored(
			&FieldDefinition {
				name: Idiom::field("plain"),
				table: TableName::from("t"),
				..Default::default()
			}
			.to_stored(),
		)
		.unwrap();
		assert!(plain.computed_logical().is_none());
		assert_eq!(plain.select_permission_logical().unwrap(), LogicalPermission::Full);
	}

	/// A `DEFAULT` is carried twice for the reason `COMPUTED` is, and both
	/// spellings carry it: a definition read back from storage compiles the
	/// same scalar as the one a statement built.
	#[test]
	fn a_default_clause_compiles_to_its_logical_form() {
		for spelling in [DefineDefault::Set, DefineDefault::Always] {
			let clause: Expr = ExprText::from_raw("time::now()").compile().unwrap();
			let def = FieldDefinition {
				name: Idiom::field("created"),
				table: TableName::from("t"),
				default_body: Some(LogicalBody::of(&clause)),
				default: spelling(clause),
				..Default::default()
			};
			let loaded = FieldDefinition::from_stored(&def.to_stored()).unwrap();
			assert_eq!(loaded, def);
			let body = loaded.default_logical().expect("a field with a DEFAULT").unwrap();
			assert_eq!(surrealdb_types::ToSql::to_sql(body), "time::now()");
		}

		let plain = FieldDefinition::from_stored(
			&FieldDefinition {
				name: Idiom::field("plain"),
				table: TableName::from("t"),
				..Default::default()
			}
			.to_stored(),
		)
		.unwrap();
		assert!(plain.default_logical().is_none());
	}

	/// A field named after any reserved keyword must survive the stored-text
	/// round-trip, as the leading segment and after a dot. The raw rendering
	/// leaves a leading keyword bare, so this is what keeps a definition that
	/// stores from becoming one that cannot be loaded — which would make the
	/// whole table's schema unreadable, not just the one field. Pinning the
	/// entire keyword set means a word added to `RESERVED_KEYWORD` cannot
	/// reintroduce that.
	#[test]
	fn every_reserved_keyword_round_trips_as_a_field_name() {
		for kw in common::keywords::RESERVED_KEYWORD.iter() {
			let kw = kw.into_inner();
			for name in [
				Idiom::field(kw),
				Idiom::field(kw).push(crate::expr::Part::Field(kw.into())),
				Idiom::field("meta").push(crate::expr::Part::Field(kw.into())),
			] {
				let def = FieldDefinition {
					name: name.clone(),
					table: TableName::from("t"),
					..Default::default()
				};
				let stored = def.to_stored();
				assert_eq!(
					FieldDefinition::from_stored(&stored).unwrap_or_else(|e| panic!(
						"keyword field `{kw}` stored as `{}` did not load: {e}",
						stored.name.as_str()
					)),
					def,
					"keyword field `{kw}` did not survive to_stored/from_stored"
				);
			}
		}
	}

	fn component_names(list: &[&str]) -> KeyNames {
		KeyNames::new(list.iter().map(surrealdb_strand::Strand::new).collect(), list.len()).unwrap()
	}

	fn tuple(kinds: Vec<Kind>) -> Kind {
		Kind::Literal(crate::expr::KindLiteral::Array(kinds))
	}

	fn id_field(kind: Kind, names: Option<KeyNames>) -> FieldDefinition {
		let mut def = FieldDefinition {
			name: Idiom::field("id"),
			table: TableName::from("reading"),
			..Default::default()
		};
		def.set_type_strict(Some(kind), names).unwrap();
		def
	}

	/// The names live in the text of the type, so they survive a trip through
	/// the stored form and back, and print as the statement that defined them.
	#[test]
	fn a_named_id_survives_storage_and_rendering() {
		let cases: [(&[&str], Vec<Kind>, &str); 4] = [
			(
				&["city", "time"],
				vec![Kind::String, Kind::Datetime],
				"[city: string, time: datetime]",
			),
			(&["city"], vec![Kind::String], "[city: string]"),
			// A space needs backticks; so do the reserved words `value` and `option`.
			// `DEFINE FIELD` refuses a name that is not a plain identifier, but stored
			// text holding one still loads and renders as written.
			(&["my city", "n"], vec![Kind::String, Kind::Int], "[`my city`: string, n: int]"),
			(
				&["value", "string", "option"],
				vec![Kind::Int, Kind::String, Kind::Int],
				"[`value`: int, string: string, `option`: int]",
			),
		];
		for (list, kinds, text) in cases {
			let mut def = id_field(tuple(kinds.clone()), None);
			def.set_type(Some(tuple(kinds.clone())), Some(component_names(list)));
			let plain = list.iter().all(|name| KeyStructure::is_plain_name(name));
			assert_eq!(
				def.clone()
					.set_type_strict(Some(tuple(kinds.clone())), Some(component_names(list)))
					.is_ok(),
				plain,
				"{text}"
			);
			let key = def.key.as_ref().expect("a named id has a key structure");
			assert_eq!(key.arity(), list.len());

			let stored = def.to_stored();
			assert_eq!(stored.field_kind.as_ref().map(KindText::as_str), Some(text));

			let loaded = FieldDefinition::from_stored(&stored).unwrap();
			assert_eq!(loaded, def, "{text} did not survive to_stored/from_stored");
			assert_eq!(loaded.key, def.key);
			assert_eq!(loaded.field_kind, Some(tuple(kinds)));
			assert_eq!(loaded.to_stored(), stored, "{text} did not store back to the same bytes");

			let statement =
				loaded.to_runtime_definition(runtime::statements::define::DefineKind::Default);
			assert_eq!(statement.key_names, key.names);
			assert!(
				statement.to_sql().starts_with(&format!("DEFINE FIELD id ON reading TYPE {text}")),
				"{}",
				statement.to_sql()
			);
			assert!(
				loaded
					.to_sql_overwrite()
					.starts_with(&format!("DEFINE FIELD OVERWRITE id ON reading TYPE {text}")),
				"{}",
				loaded.to_sql_overwrite()
			);
			assert!(
				loaded.to_sql().starts_with(&format!("DEFINE FIELD id ON reading TYPE {text}"))
			);
		}
	}

	/// A type with no names is stored as the plain kind text, and reads
	/// back with no structure.
	#[test]
	fn an_unnamed_id_is_stored_as_the_plain_kind_text() {
		for kind in [
			tuple(vec![Kind::String, Kind::Int]),
			tuple(vec![Kind::Float]),
			Kind::Record(vec![TableName::from("person")]),
			Kind::Either(vec![Kind::String, Kind::Int]),
			Kind::Any,
		] {
			let def = id_field(kind.clone(), None);
			assert!(def.key.is_none() && def.key_part.is_none());
			let stored = def.to_stored();
			assert_eq!(
				stored.field_kind,
				Some(KindText::new(&kind)),
				"the stored text of `{}` changed",
				kind.to_sql()
			);
			assert_eq!(stored.field_kind.as_ref().unwrap().as_str(), kind.to_sql());

			let loaded = FieldDefinition::from_stored(&stored).unwrap();
			assert_eq!(loaded, def);
			assert!(loaded.key.is_none());
			assert_eq!(
				loaded
					.to_runtime_definition(runtime::statements::define::DefineKind::Default)
					.key_names,
				None
			);
		}

		// A field with no type at all.
		let bare = FieldDefinition {
			name: Idiom::field("id"),
			table: TableName::from("reading"),
			..Default::default()
		};
		assert_eq!(bare.to_stored().field_kind, None);
		assert_eq!(FieldDefinition::from_stored(&bare.to_stored()).unwrap(), bare);
	}

	/// A declared SQL type survives the stored form and its encoded entry, and a
	/// definition without one holds no `sql_type` entry, so an earlier reader sees
	/// the bytes it always did.
	#[test]
	fn sql_type_round_trips_through_the_stored_form_and_its_entry() {
		use crate::stored::private::KVValue;

		let mut definition = FieldDefinition {
			name: Idiom::field("n"),
			table: TableName::from("t"),
			..Default::default()
		};
		let plain = definition.to_stored();
		assert_eq!(plain.sql_type, None);

		definition.sql_type = Some(SqlType {
			dialect: SqlTypeDialect::PostgreSql,
			text: "numeric(10,2)".to_owned(),
		});
		let stored = definition.to_stored();
		assert_eq!(FieldDefinition::from_stored(&stored).unwrap(), definition);

		let encoded = stored.kv_encode_value().unwrap();
		let decoded = StoredFieldDefinition::kv_decode_value(&encoded, ()).unwrap();
		assert_eq!(decoded, stored);
		assert_eq!(FieldDefinition::from_stored(&decoded).unwrap(), definition);

		for (dialect, expected) in [
			(SqlTypeDialect::PostgreSql, StoredSqlTypeDialect::PostgreSql),
			(SqlTypeDialect::MySql, StoredSqlTypeDialect::MySql),
			(SqlTypeDialect::DynamoDb, StoredSqlTypeDialect::DynamoDb),
		] {
			definition.sql_type = Some(SqlType {
				dialect,
				text: "t".to_owned(),
			});
			let stored_type = definition.to_stored().sql_type.unwrap();
			assert_eq!(stored_type.dialect, expected);
			assert_eq!(stored_type.text, "t");
		}
	}

	/// The names are read only for `id`: text on any other field is the plain
	/// kind grammar, which refuses names.
	#[test]
	fn names_are_read_only_from_the_type_of_id() {
		let stored = |name: &str, text: &str| StoredFieldDefinition {
			name: IdiomText::from_raw(name),
			table: TableName::from("reading"),
			field_kind: Some(KindText::from_raw(text)),
			..{
				let mut s = FieldDefinition {
					name: Idiom::field(name),
					table: TableName::from("reading"),
					..Default::default()
				}
				.to_stored();
				s.field_kind = None;
				s
			}
		};
		let id =
			FieldDefinition::from_stored(&stored("id", "[city: string, time: datetime]")).unwrap();
		assert_eq!(id.key.as_ref().map(|k| k.arity()), Some(2));
		assert_eq!(
			id.field_kind,
			Some(tuple(vec![Kind::String, Kind::Datetime])),
			"the type is the plain tuple, whatever the names"
		);

		let other = stored("value", "[city: string, time: datetime]");
		assert!(FieldDefinition::from_stored(&other).is_err());
		let plain = FieldDefinition::from_stored(&stored("value", "[string, datetime]")).unwrap();
		assert!(plain.key.is_none());
	}

	/// A stored type outside the allow-list keeps the table readable: the
	/// structure exists and orders nothing.
	#[test]
	fn a_stored_type_outside_the_allow_list_is_read_with_an_unknown_order() {
		let mut stored = id_field(tuple(vec![Kind::String, Kind::Datetime]), None).to_stored();
		stored.field_kind = Some(KindText::from_raw("[a: float, b: datetime]"));
		let loaded = FieldDefinition::from_stored(&stored).unwrap();
		let key = loaded.key.as_ref().expect("the names are kept");
		assert_eq!(key.order(0), crate::catalog::ComponentOrder::Unknown);
		assert_eq!(key.order(1), crate::catalog::ComponentOrder::CoarseSecond);
		assert_eq!(loaded.to_stored(), stored, "the text is written back as it was read");
	}

	/// Stored text whose names cannot form a valid list (a repeat, a reserved name)
	/// leaves the field readable: the type is the plain tuple and there is no
	/// structure. The whole field list of the table still loads, and nothing is
	/// derived from the id.
	#[test]
	fn stored_names_that_are_not_valid_leave_the_table_readable() {
		for (text, kinds) in [
			("[a: string, a: int]", vec![Kind::String, Kind::Int]),
			("[id: string, b: int]", vec![Kind::String, Kind::Int]),
			("[in: string]", vec![Kind::String]),
			("[x: int, out: datetime]", vec![Kind::Int, Kind::Datetime]),
		] {
			let mut id = id_field(tuple(kinds.clone()), None).to_stored();
			id.field_kind = Some(KindText::from_raw(text));
			let value = FieldDefinition {
				name: Idiom::field("value"),
				table: TableName::from("reading"),
				..Default::default()
			}
			.to_stored();
			let list = [id, value];

			let loaded = FieldDefinition::from_stored(&list[0])
				.unwrap_or_else(|e| panic!("`{text}` did not load: {e}"));
			assert_eq!(loaded.field_kind, Some(tuple(kinds)), "{text}");
			assert!(loaded.key.is_none() && loaded.live_key().is_none(), "{text}");
			assert_eq!(
				loaded.to_stored().field_kind.as_ref().map(KindText::as_str),
				Some(loaded.field_kind.as_ref().unwrap().to_sql().as_str()),
				"{text}: the names are not written back"
			);

			let fields = crate::catalog::fields_from_stored(&list)
				.unwrap_or_else(|e| panic!("`{text}`: the field list did not load: {e}"));
			assert_eq!(fields.len(), 2, "{text}");
			assert!(fields.iter().all(|f| f.key.is_none() && f.key_part.is_none()), "{text}");
		}
	}

	/// `set_type` never fails: names that do not fit are dropped and the type is
	/// kept, so a catalog read is never refused over them.
	#[test]
	fn set_type_drops_names_that_do_not_fit() {
		let mut def = FieldDefinition {
			name: Idiom::field("id"),
			table: TableName::from("reading"),
			..Default::default()
		};
		// Not a tuple.
		def.set_type(Some(Kind::String), Some(component_names(&["a"])));
		assert_eq!(def.field_kind, Some(Kind::String));
		assert!(def.key.is_none());
		// Wrong arity.
		def.set_type(Some(tuple(vec![Kind::String, Kind::Int])), Some(component_names(&["a"])));
		assert_eq!(def.field_kind, Some(tuple(vec![Kind::String, Kind::Int])));
		assert!(def.key.is_none());
		// No type at all.
		def.set_type(None, Some(component_names(&["a"])));
		assert_eq!(def.field_kind, None);
		assert!(def.key.is_none());

		// Another field.
		let mut other = FieldDefinition {
			name: Idiom::field("city"),
			table: TableName::from("reading"),
			..Default::default()
		};
		other.set_type(Some(tuple(vec![Kind::String])), Some(component_names(&["a"])));
		assert_eq!(other.field_kind, Some(tuple(vec![Kind::String])));
		assert!(other.key.is_none());

		// A later call replaces the structure, and `None` clears it.
		def.set_type(Some(tuple(vec![Kind::String])), Some(component_names(&["a"])));
		assert!(def.key.is_some());
		def.set_type(Some(tuple(vec![Kind::String])), None);
		assert!(def.key.is_none());
	}

	#[test]
	fn set_type_strict_refuses_what_a_key_range_cannot_reason_about() {
		let mut def = id_field(tuple(vec![Kind::String]), Some(component_names(&["a"])));
		let before = def.clone();
		assert_eq!(
			def.set_type_strict(
				Some(tuple(vec![Kind::String, Kind::Float])),
				Some(component_names(&["a", "b"]))
			),
			Err(KeyStructureError::UnsupportedKind {
				name: "b".to_owned(),
				kind: "float".to_owned()
			})
		);
		assert_eq!(
			def.set_type_strict(Some(Kind::String), Some(component_names(&["a"]))),
			Err(KeyStructureError::NotATuple)
		);
		assert_eq!(
			def.set_type_strict(None, Some(component_names(&["a"]))),
			Err(KeyStructureError::NotATuple)
		);
		assert_eq!(def, before, "a refused type leaves the definition as it was");

		// Without names there is nothing to refuse, whatever the type.
		def.set_type_strict(Some(tuple(vec![Kind::Float])), None).unwrap();
		assert!(def.key.is_none());
	}

	/// INFO FOR TABLE ... STRUCTURE lists the components of a named id, and
	/// nothing for any other definition.
	#[test]
	fn structure_lists_the_components_of_a_named_id() {
		let get = |v: &Value, key: &str| {
			let Value::Object(o) = v else {
				panic!("expected an object, got {v:?}");
			};
			o.get(key).cloned()
		};
		let named = id_field(
			tuple(vec![Kind::String, Kind::Datetime]),
			Some(component_names(&["city", "time"])),
		)
		.structure();
		assert_eq!(get(&named, "kind"), Some(Value::from("[string, datetime]")));
		let Some(Value::Array(components)) = get(&named, "components") else {
			panic!("a named id reports its components");
		};
		assert_eq!(components.0.len(), 2);
		for (position, (name, kind)) in
			[("city", "string"), ("time", "datetime")].into_iter().enumerate()
		{
			let c = &components.0[position];
			assert_eq!(get(c, "name"), Some(Value::from(name)));
			assert_eq!(get(c, "position"), Some(Value::from(position as i64)));
			assert_eq!(get(c, "kind"), Some(Value::from(kind)));
		}

		let unnamed = id_field(tuple(vec![Kind::String, Kind::Datetime]), None).structure();
		assert_eq!(get(&unnamed, "components"), None);
		let plain = FieldDefinition {
			name: Idiom::field("value"),
			table: TableName::from("reading"),
			..Default::default()
		}
		.structure();
		assert_eq!(get(&plain, "components"), None);
	}

	/// A virtual definition has no stored form. Handing one to `to_stored`
	/// is a fault in the caller, which a debug build catches.
	#[test]
	#[cfg(debug_assertions)]
	#[should_panic(expected = "no stored form")]
	fn a_derived_definition_cannot_be_stored() {
		let key =
			id_field(tuple(vec![Kind::String]), Some(component_names(&["city"]))).key.unwrap();
		let derived = key.derived_fields(&TableName::from("reading")).remove(0);
		assert!(derived.is_key_part());
		let _ = derived.to_stored();
	}

	/// The structure is derived from the type. A type written to `field_kind`
	/// directly leaves it stale, and what is stored, rendered and reported is
	/// the type.
	#[test]
	fn a_type_written_directly_wins_over_a_stale_structure() {
		let mut def = id_field(
			tuple(vec![Kind::String, Kind::Datetime]),
			Some(component_names(&["city", "time"])),
		);
		def.field_kind = Some(tuple(vec![Kind::String, Kind::Int]));

		let stored = def.to_stored();
		assert_eq!(stored.field_kind.as_ref().map(KindText::as_str), Some("[string, int]"));
		assert!(FieldDefinition::from_stored(&stored).unwrap().key.is_none());

		let statement = def.to_runtime_definition(runtime::statements::define::DefineKind::Default);
		assert_eq!(statement.key_names, None);
		assert!(statement.to_sql().starts_with("DEFINE FIELD id ON reading TYPE [string, int]"));

		let Value::Object(info) = def.structure() else {
			panic!("a definition reports an object");
		};
		assert_eq!(info.get("components"), None);
	}
}
