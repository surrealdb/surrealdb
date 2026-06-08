use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use super::Permission;
use crate::catalog::auth::AuthLimit;
use crate::expr::reference::Reference;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Idiom, Kind};
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::{self, DefineFieldStatement};
use crate::val::{TableName, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) enum DefineDefault {
	#[default]
	None,
	Always(Expr),
	Set(Expr),
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

#[revisioned(revision = 4)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct FieldDefinition {
	// TODO: Needs to be it's own type.
	// Idiom::Value/Idiom::Start are for example not allowed.
	pub(crate) name: Idiom,
	pub(crate) table: TableName,
	// TODO: Optionally also be a separate type from expr::Kind
	pub(crate) field_kind: Option<Kind>,
	pub(crate) flexible: bool,
	pub(crate) readonly: bool,
	pub(crate) value: Option<Expr>,
	pub(crate) assert: Option<Expr>,
	pub(crate) computed: Option<Expr>,
	pub(crate) default: DefineDefault,

	pub(crate) select_permission: Permission,
	pub(crate) create_permission: Permission,
	pub(crate) update_permission: Permission,

	pub(crate) comment: Option<String>,
	pub(crate) reference: Option<Reference>,

	/// The auth limit of the API.
	#[revision(start = 2, default_fn = "default_auth_limit")]
	pub(crate) auth_limit: AuthLimit,

	/// Pre-computed dependency metadata for computed fields.
	/// `None` for non-computed fields or legacy definitions (pre-revision 3).
	/// When `None` on a computed field, deps are extracted on-the-fly at query time.
	#[revision(start = 3, default_fn = "default_computed_deps")]
	pub(crate) computed_deps: Option<ComputedDeps>,

	/// Optional alias used as the GraphQL field name. When set, GraphQL
	/// schema generation prefers this over the raw SurrealQL field name,
	/// allowing snake_case columns to be exposed as camelCase. See
	/// GitHub issue #4537. `Option<String>::default()` already returns
	/// `None` so no explicit `default_fn` is needed.
	#[revision(start = 4)]
	pub(crate) graphql_alias: Option<String>,

	/// Reason emitted on the GraphQL `@deprecated` directive. When set, the
	/// corresponding GraphQL field is marked deprecated in introspection
	/// (and in input objects), surfacing the reason to schema consumers
	/// while remaining usable for backwards compatibility.
	#[revision(start = 4)]
	pub(crate) graphql_deprecated: Option<String>,
}

impl FieldDefinition {
	// This was pushed in after the first beta, so we need to add auth_limit to structs in a
	// non-breaking way
	fn default_auth_limit(_revision: u16) -> Result<AuthLimit, revision::Error> {
		Ok(AuthLimit::new_no_limit())
	}

	fn default_computed_deps(_revision: u16) -> Result<Option<ComputedDeps>, revision::Error> {
		Ok(None)
	}
}
impl_kv_value_revisioned!(FieldDefinition);

impl FieldDefinition {
	pub fn to_sql_definition(&self) -> DefineFieldStatement {
		DefineFieldStatement {
			kind: sql::statements::define::DefineKind::Default,
			name: Expr::Idiom(self.name.clone()).into(),
			what: sql::Expr::Table(self.table.clone()),
			field_kind: self.field_kind.clone().map(|x| x.into()),
			flexible: self.flexible,
			readonly: self.readonly,
			value: self.value.clone().map(|x| x.into()),
			assert: self.assert.clone().map(|x| x.into()),
			computed: self.computed.clone().map(|x| x.into()),
			default: match &self.default {
				DefineDefault::None => sql::statements::define::DefineDefault::None,
				DefineDefault::Set(x) => {
					sql::statements::define::DefineDefault::Set(x.clone().into())
				}
				DefineDefault::Always(x) => {
					sql::statements::define::DefineDefault::Always(x.clone().into())
				}
			},
			permissions: sql::Permissions {
				select: self.select_permission.to_sql_definition(),
				create: self.create_permission.to_sql_definition(),
				update: self.update_permission.to_sql_definition(),
				delete: sql::Permission::Full,
			},
			comment: self
				.comment
				.clone()
				.map(|x| sql::Expr::Literal(sql::Literal::String(x.into())))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
			reference: self.reference.clone().map(|x| x.into()),
			graphql_alias: self.graphql_alias.clone(),
			graphql_deprecated: self.graphql_deprecated.clone(),
		}
	}
}

impl InfoStructure for FieldDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name" => self.name.structure(),
			"table" => Value::String(self.table.into()),
			"kind", if let Some(v) = self.field_kind => v.structure(),
			"flexible", if self.flexible => true.into(),
			"value", if let Some(v) = self.value => v.structure(),
			"assert", if let Some(v) = self.assert => v.structure(),
			"computed", if let Some(v) = self.computed => v.structure(),
			"default_always", if matches!(&self.default, DefineDefault::Always(_) | DefineDefault::Set(_)) => Value::Bool(matches!(self.default,DefineDefault::Always(_))), // Only reported if DEFAULT is also enabled for this field
			"default", if let DefineDefault::Always(v) | DefineDefault::Set(v) = self.default => v.structure(),
			"reference", if let Some(v) = self.reference => v.structure(),
			"readonly" => self.readonly.into(),
			"permissions" => Value::from(map!{
				"select" => self.select_permission.structure(),
				"create" => self.create_permission.structure(),
				"update" => self.update_permission.structure(),
			}),
			"comment", if let Some(v) = self.comment => v.into(),
			"graphql_alias", if let Some(v) = self.graphql_alias => v.into(),
			"graphql_deprecated", if let Some(v) = self.graphql_deprecated => v.into(),
		})
	}
}

impl ToSql for FieldDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}
