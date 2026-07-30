use anyhow::Context as _;
use revision::revisioned;
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use super::{StoredPermission, StoredReference};
use crate::catalog::auth::AuthLimit;
use crate::catalog::{ExprText, FromStored, IdiomText, KindText, Permission, reference_to_stored};
use crate::expr::reference::Reference as ExprReference;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Idiom, Kind};
use crate::sql;
use crate::val::{TableName, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum StoredDefineDefault {
	#[default]
	None,
	/// Canonical SurrealQL text of the default-value expression.
	Always(ExprText),
	Set(ExprText),
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

#[revisioned(revision = 5)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StoredFieldDefinition {
	/// Canonical SurrealQL text of the field's path.
	///
	/// TODO: Needs to be its own grammar rather than a whole idiom. A field
	/// path may only be a chain of name/`*`/destructure parts; the parser
	/// this text compiles through (`syn::idiom_for_definition`) still admits
	/// shapes a field path has no meaning for, such as a `[WHERE ...]` filter
	/// or a computed `[$expr]` index, so the restriction is enforced by the
	/// `DEFINE FIELD` parser rather than by the type.
	pub name: IdiomText,
	pub table: TableName,
	/// Canonical SurrealQL kind-grammar text of the `TYPE` clause.
	#[revision(end = 5, convert_fn = "convert_field_kind")]
	pub old_field_kind: Option<Kind>,
	#[revision(start = 5)]
	pub field_kind: Option<KindText>,
	pub flexible: bool,
	pub readonly: bool,
	/// Canonical SurrealQL text of the `VALUE` clause's expression.
	pub value: Option<ExprText>,
	/// Canonical SurrealQL text of the `ASSERT` clause's expression.
	pub assert: Option<ExprText>,
	/// Canonical SurrealQL text of the `COMPUTED` clause's expression.
	pub computed: Option<ExprText>,
	pub default: StoredDefineDefault,

	pub select_permission: StoredPermission,
	pub create_permission: StoredPermission,
	pub update_permission: StoredPermission,

	pub comment: Option<String>,
	pub reference: Option<StoredReference>,

	/// The auth limit of the API.
	#[revision(start = 2, default_fn = "default_auth_limit")]
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
	pub graphql_alias: Option<String>,

	/// Reason emitted on the GraphQL `@deprecated` directive. When set, the
	/// corresponding GraphQL field is marked deprecated in introspection
	/// (and in input objects), surfacing the reason to schema consumers
	/// while remaining usable for backwards compatibility.
	#[revision(start = 4)]
	pub graphql_deprecated: Option<String>,
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
impl_kv_value_revisioned!(StoredFieldDefinition);

impl InfoStructure for FieldDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name" => self.name.structure(),
			"table" => Value::String(self.table.into()),
			"kind", if let Some(v) = self.field_kind => Value::from(v.to_sql()),
			"flexible", if self.flexible => true.into(),
			"value", if let Some(v) = self.value => Value::from(v.to_stored_sql()),
			"assert", if let Some(v) = self.assert => Value::from(v.to_stored_sql()),
			"computed", if let Some(v) = self.computed => Value::from(v.to_stored_sql()),
			"default_always", if matches!(&self.default, DefineDefault::Always(_) | DefineDefault::Set(_)) => Value::Bool(matches!(self.default, DefineDefault::Always(_))), // Only reported if DEFAULT is also enabled for this field
			"default", if let DefineDefault::Always(v) | DefineDefault::Set(v) = self.default => Value::from(v.to_stored_sql()),
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

impl FieldDefinition {
	/// Lowers to the sql-side statement, which is what renders.
	///
	/// One renderer, not two: a clause added to `sql::DefineFieldStatement`
	/// alone would otherwise be echoed back for a user-typed statement but
	/// silently dropped from `INFO` and from export, which renders the catalog
	/// type — so the dump would no longer reproduce the schema.
	pub fn to_sql_definition(
		&self,
		kind: sql::statements::define::DefineKind,
	) -> sql::statements::define::DefineFieldStatement {
		sql::statements::define::DefineFieldStatement {
			kind,
			name: crate::expr::Expr::Idiom(self.name.clone()).into(),
			what: sql::Expr::Table(self.table.clone().into()),
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
				select: self.select_permission.to_sql_permission(),
				create: self.create_permission.to_sql_permission(),
				update: self.update_permission.to_sql_permission(),
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

	/// Renders as `DEFINE FIELD OVERWRITE ...`, for idempotent re-import via
	/// export (relation tables auto-generate in/out fields, and array types
	/// generate sub-field definitions that would conflict on plain re-DEFINE).
	pub fn to_sql_overwrite(&self) -> String {
		self.to_sql_definition(sql::statements::define::DefineKind::Overwrite).to_sql()
	}
}

impl ToSql for FieldDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition(sql::statements::define::DefineKind::Default).fmt_sql(f, fmt)
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
	pub fn to_stored(&self) -> StoredDefineDefault {
		match self {
			DefineDefault::None => StoredDefineDefault::None,
			DefineDefault::Always(e) => StoredDefineDefault::Always(ExprText::new(e)),
			DefineDefault::Set(e) => StoredDefineDefault::Set(ExprText::new(e)),
		}
	}
}

/// Runtime form of [`StoredFieldDefinition`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FieldDefinition {
	pub name: Idiom,
	pub table: TableName,
	pub field_kind: Option<Kind>,
	pub flexible: bool,
	pub readonly: bool,
	pub value: Option<Expr>,
	pub assert: Option<Expr>,
	pub computed: Option<Expr>,
	pub default: DefineDefault,
	pub select_permission: Permission,
	pub create_permission: Permission,
	pub update_permission: Permission,
	pub reference: Option<ExprReference>,
	pub auth_limit: AuthLimit,
	pub comment: Option<String>,
	pub graphql_alias: Option<String>,
	pub graphql_deprecated: Option<String>,
}

impl FromStored for FieldDefinition {
	type Stored = StoredFieldDefinition;

	fn from_stored(stored: &StoredFieldDefinition) -> anyhow::Result<FieldDefinition> {
		fn build(stored: &StoredFieldDefinition) -> anyhow::Result<FieldDefinition> {
			Ok(FieldDefinition {
				name: stored.name.compile()?,
				table: stored.table.clone(),
				field_kind: stored.field_kind.as_ref().map(|t| t.compile()).transpose()?,
				flexible: stored.flexible,
				readonly: stored.readonly,
				value: stored.value.as_ref().map(|t| t.compile()).transpose()?,
				assert: stored.assert.as_ref().map(|t| t.compile()).transpose()?,
				computed: stored.computed.as_ref().map(|t| t.compile()).transpose()?,
				default: DefineDefault::from_stored(&stored.default)?,
				select_permission: Permission::from_stored(&stored.select_permission)?,
				create_permission: Permission::from_stored(&stored.create_permission)?,
				update_permission: Permission::from_stored(&stored.update_permission)?,
				reference: stored.reference.as_ref().map(ExprReference::from_stored).transpose()?,
				auth_limit: stored.auth_limit.clone(),
				comment: stored.comment.clone(),
				graphql_alias: stored.graphql_alias.clone(),
				graphql_deprecated: stored.graphql_deprecated.clone(),
			})
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
	pub fn to_stored(&self) -> StoredFieldDefinition {
		StoredFieldDefinition {
			name: IdiomText::new(&self.name),
			table: self.table.clone(),
			field_kind: self.field_kind.as_ref().map(KindText::new),
			flexible: self.flexible,
			readonly: self.readonly,
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
		}
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
}
