use anyhow::Context as _;
use revision::revisioned;
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::auth::AuthLimit;
use crate::catalog::{BlockText, FromStored, KindText, Permission, StoredPermission};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Block, Kind};
use crate::sql::statements::define::DefineKind;
use crate::sql::{self, DefineFunctionStatement};
use crate::val::Value;

#[revisioned(revision = 4)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StoredFunctionDefinition {
	pub name: Strand,
	/// Canonical SurrealQL kind-grammar text for each argument's declared type.
	#[revision(end = 4, convert_fn = "convert_args")]
	pub old_args: Vec<(String, Kind)>,
	#[revision(start = 4)]
	pub args: Vec<(String, KindText)>,
	/// Canonical SurrealQL text of the function's `{ ... }` body block.
	pub block: BlockText,
	pub comment: Option<String>,
	pub permissions: StoredPermission,
	/// Canonical SurrealQL kind-grammar text of the `RETURNS` clause.
	#[revision(end = 4, convert_fn = "convert_returns")]
	pub old_returns: Option<Kind>,
	#[revision(start = 4)]
	pub returns: Option<KindText>,
	/// The auth limit of the API.
	#[revision(start = 2, default_fn = "default_auth_limit")]
	pub auth_limit: AuthLimit,
	/// Optional alias used as the GraphQL Query field name. See GitHub issue
	/// #4537. `Option<String>::default()` already returns `None`.
	#[revision(start = 3)]
	pub graphql_alias: Option<String>,

	/// Reason emitted on the GraphQL `@deprecated` directive of the
	/// auto-generated Query field for this function.
	#[revision(start = 3)]
	pub graphql_deprecated: Option<String>,
}

// This was pushed in after the first beta, so we need to add auth_limit to structs in a
// non-breaking way
impl StoredFunctionDefinition {
	fn default_auth_limit(_revision: u16) -> Result<AuthLimit, revision::Error> {
		Ok(AuthLimit::new_no_limit())
	}

	/// Renders each argument's old structured `Kind` to canonical SurrealQL
	/// kind-grammar text, matching what `Kind::to_sql()` already produces.
	fn convert_args(
		&mut self,
		_rev: u16,
		value: Vec<(String, Kind)>,
	) -> Result<(), revision::Error> {
		self.args = value.into_iter().map(|(n, k)| (n, KindText::new(&k))).collect();
		Ok(())
	}

	/// Renders the old structured `RETURNS` `Kind` to canonical SurrealQL
	/// kind-grammar text.
	fn convert_returns(&mut self, _rev: u16, value: Option<Kind>) -> Result<(), revision::Error> {
		self.returns = value.map(|k| KindText::new(&k));
		Ok(())
	}
}

impl_kv_value_revisioned!(StoredFunctionDefinition);

impl FunctionDefinition {
	fn to_sql_definition(&self) -> DefineFunctionStatement {
		DefineFunctionStatement {
			kind: DefineKind::Default,
			name: self.name.clone(),
			args: self.args.iter().map(|(n, k)| (n.clone(), sql::Kind::from(k.clone()))).collect(),
			block: self.block.clone().into(),
			permissions: self.permissions.to_sql_permission(),
			returns: self.returns.as_ref().map(|k| sql::Kind::from(k.clone())),
			comment: self
				.comment
				.clone()
				.map(|x| sql::Expr::Literal(sql::Literal::String(x.into())))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
			graphql_alias: self.graphql_alias.clone(),
			graphql_deprecated: self.graphql_deprecated.clone(),
		}
	}
}

impl InfoStructure for FunctionDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name" => self.name.into(),
			"args" => self.args
				.into_iter()
				.map(|(n, k)| vec![n.into(), Value::from(k.to_sql())].into())
				.collect::<Vec<Value>>()
				.into(),
			"block" => Value::from(self.block.to_sql()),
			"permissions" => self.permissions.structure(),
			"comment", if let Some(v) = self.comment => v.to_sql().into(),
			"returns", if let Some(v) = self.returns => Value::from(v.to_sql()),
			"graphql_alias", if let Some(v) = self.graphql_alias => v.into(),
			"graphql_deprecated", if let Some(v) = self.graphql_deprecated => v.into(),
		})
	}
}

impl ToSql for FunctionDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}

/// Runtime form of [`StoredFunctionDefinition`].
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FunctionDefinition {
	pub name: Strand,
	pub args: Vec<(String, Kind)>,
	pub block: Block,
	pub returns: Option<Kind>,
	pub permissions: Permission,
	pub auth_limit: AuthLimit,
	pub comment: Option<String>,
	pub graphql_alias: Option<String>,
	pub graphql_deprecated: Option<String>,
}

impl FromStored for FunctionDefinition {
	type Stored = StoredFunctionDefinition;

	fn from_stored(stored: &StoredFunctionDefinition) -> anyhow::Result<FunctionDefinition> {
		fn build(stored: &StoredFunctionDefinition) -> anyhow::Result<FunctionDefinition> {
			Ok(FunctionDefinition {
				name: stored.name.clone(),
				args: stored
					.args
					.iter()
					.map(|(n, t)| Ok((n.clone(), t.compile()?)))
					.collect::<anyhow::Result<_>>()?,
				block: stored.block.compile()?,
				returns: stored.returns.as_ref().map(|t| t.compile()).transpose()?,
				permissions: Permission::from_stored(&stored.permissions)?,
				auth_limit: stored.auth_limit.clone(),
				comment: stored.comment.clone(),
				graphql_alias: stored.graphql_alias.clone(),
				graphql_deprecated: stored.graphql_deprecated.clone(),
			})
		}
		build(stored).with_context(|| {
			format!("the stored definition of function `fn::{}` no longer compiles", stored.name)
		})
	}
}

impl FunctionDefinition {
	pub fn to_stored(&self) -> StoredFunctionDefinition {
		StoredFunctionDefinition {
			name: self.name.clone(),
			args: self.args.iter().map(|(n, k)| (n.clone(), KindText::new(k))).collect(),
			block: BlockText::new(&self.block),
			comment: self.comment.clone(),
			permissions: crate::catalog::StoredPermission::from_runtime(&self.permissions),
			returns: self.returns.as_ref().map(KindText::new),
			auth_limit: self.auth_limit.clone(),
			graphql_alias: self.graphql_alias.clone(),
			graphql_deprecated: self.graphql_deprecated.clone(),
		}
	}
}
