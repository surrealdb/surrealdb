use anyhow::{Context as _, Result};
use common::fmt::EscapeKwFreeIdent;
use revision::revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::catalog::{ApiConfig, FromStored, StoredApiConfigDefinition};
use crate::expr::statements::info::InfoStructure;
use crate::iam::ConfigKind;
use crate::key::impl_kv_value_revisioned;
use crate::sql;
use crate::val::{TableName, Value};

/// The config struct as it is stored on disk.
///
/// Only `Api` has a `Stored` twin. `DefaultConfig` and `GraphQLConfig` contain no
/// SurrealQL — they bottom out in `String`, `Strand`, `TableName` and `u32` — so
/// their stored and runtime forms are the same type, and [`FromStored`] clones
/// them through unchanged. `GraphQLConfig` is additionally the payload of the
/// expr-layer `ConfigInner::GraphQL` and a component of the GraphQL schema cache
/// key, so a `Stored` prefix would misdescribe it in most of its uses.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum StoredConfigDefinition {
	Default(DefaultConfig),
	GraphQL(GraphQLConfig),
	Api(StoredApiConfigDefinition),
}
impl_kv_value_revisioned!(StoredConfigDefinition);

impl StoredConfigDefinition {
	/// Convert the config definition into a graphql config.
	#[allow(unused)]
	pub fn try_into_graphql(self) -> Result<GraphQLConfig> {
		match self {
			StoredConfigDefinition::GraphQL(g) => Ok(g),
			c => fail!("found {} when a graphql config was expected", c.to_sql()),
		}
	}
}

impl ToSql for StoredConfigDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match &self {
			StoredConfigDefinition::Default(v) => v.fmt_sql(f, fmt),
			StoredConfigDefinition::GraphQL(v) => {
				let sql_config: crate::sql::statements::define::config::GraphQLConfig =
					v.clone().into();
				sql_config.fmt_sql(f, fmt)
			}
			StoredConfigDefinition::Api(v) => v.fmt_sql(f, fmt),
		}
	}
}

impl ConfigDefinition {
	/// See [`StoredConfigDefinition::name`]; same name derivation on the compiled
	/// form.
	pub(crate) fn name(&self) -> String {
		match self {
			ConfigDefinition::Default(_) => ConfigKind::Default.to_string(),
			ConfigDefinition::GraphQL(_) => ConfigKind::GraphQL.to_string(),
			ConfigDefinition::Api(_) => ConfigKind::Api.to_string(),
		}
	}
}

impl ToSql for ConfigDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match &self {
			ConfigDefinition::Default(v) => v.fmt_sql(f, fmt),
			ConfigDefinition::GraphQL(v) => {
				let sql_config: crate::sql::statements::define::config::GraphQLConfig =
					v.clone().into();
				sql_config.fmt_sql(f, fmt)
			}
			ConfigDefinition::Api(v) => v.fmt_sql(f, fmt),
		}
	}
}

impl InfoStructure for ConfigDefinition {
	fn structure(self) -> Value {
		match self {
			ConfigDefinition::Default(v) => Value::from(map!(
				"defaults" => v.structure()
			)),
			ConfigDefinition::GraphQL(v) => Value::from(map!(
				"graphql" => v.structure()
			)),
			ConfigDefinition::Api(v) => Value::from(map!(
				"api" => v.structure()
			)),
		}
	}
}

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct GraphQLConfig {
	pub tables: GraphQLTablesConfig,
	pub functions: GraphQLFunctionsConfig,
	/// Maximum query nesting depth. `None` means no limit.
	#[revision(start = 2)]
	pub depth_limit: Option<u32>,
	/// Maximum query complexity (total number of fields). `None` means no limit.
	#[revision(start = 2)]
	pub complexity_limit: Option<u32>,
	/// Controls whether GraphQL schema introspection is enabled.
	/// Defaults to `Auto` (introspection enabled).
	#[revision(start = 3)]
	pub introspection: GraphQLIntrospectionConfig,
}

impl InfoStructure for GraphQLConfig {
	fn structure(self) -> Value {
		Value::from(map!(
			"tables" => self.tables.structure(),
			"functions" => self.functions.structure(),
			"depth_limit", if let Some(d) = self.depth_limit => Value::from(d as i64),
			"complexity_limit", if let Some(c) = self.complexity_limit => Value::from(c as i64),
			"introspection", if let GraphQLIntrospectionConfig::None = self.introspection => Value::None,
		))
	}
}

impl From<sql::statements::define::config::graphql::GraphQLConfig> for GraphQLConfig {
	fn from(v: sql::statements::define::config::graphql::GraphQLConfig) -> Self {
		GraphQLConfig {
			tables: v.tables.into(),
			functions: v.functions.into(),
			depth_limit: v.depth_limit,
			complexity_limit: v.complexity_limit,
			introspection: v.introspection.into(),
		}
	}
}

impl From<GraphQLConfig> for sql::statements::define::config::graphql::GraphQLConfig {
	fn from(v: GraphQLConfig) -> Self {
		Self {
			tables: v.tables.into(),
			functions: v.functions.into(),
			depth_limit: v.depth_limit,
			complexity_limit: v.complexity_limit,
			introspection: v.introspection.into(),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum GraphQLTablesConfig {
	#[default]
	None,
	Auto,
	Include(Vec<TableName>),
	Exclude(Vec<TableName>),
}

impl InfoStructure for GraphQLTablesConfig {
	fn structure(self) -> Value {
		match self {
			GraphQLTablesConfig::None => Value::None,
			GraphQLTablesConfig::Auto => Value::String(Strand::new_static("AUTO")),
			GraphQLTablesConfig::Include(ts) => Value::from(map!(
				"include" => Value::Array(ts.into_iter().map(Value::Table).collect()),
			)),
			GraphQLTablesConfig::Exclude(ts) => Value::from(map!(
				"exclude" => Value::Array(ts.into_iter().map(Value::Table).collect()),
			)),
		}
	}
}

impl From<sql::statements::define::config::graphql::TablesConfig> for GraphQLTablesConfig {
	fn from(v: sql::statements::define::config::graphql::TablesConfig) -> Self {
		use sql::statements::define::config::graphql::TablesConfig;
		match v {
			TablesConfig::None => Self::None,
			TablesConfig::Auto => Self::Auto,
			TablesConfig::Include(cs) => {
				Self::Include(cs.into_iter().map(|t| t.name.into()).collect())
			}
			TablesConfig::Exclude(cs) => {
				Self::Exclude(cs.into_iter().map(|t| t.name.into()).collect())
			}
		}
	}
}

impl From<GraphQLTablesConfig> for sql::statements::define::config::graphql::TablesConfig {
	fn from(v: GraphQLTablesConfig) -> Self {
		use sql::statements::define::config::graphql::TableConfig;
		match v {
			GraphQLTablesConfig::None => Self::None,
			GraphQLTablesConfig::Auto => Self::Auto,
			GraphQLTablesConfig::Include(cs) => Self::Include(
				cs.into_iter()
					.map(|t| TableConfig {
						name: t.into(),
					})
					.collect(),
			),
			GraphQLTablesConfig::Exclude(cs) => Self::Exclude(
				cs.into_iter()
					.map(|t| TableConfig {
						name: t.into(),
					})
					.collect(),
			),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum GraphQLFunctionsConfig {
	#[default]
	None,
	Auto,
	Include(Vec<Strand>),
	Exclude(Vec<Strand>),
}

impl InfoStructure for GraphQLFunctionsConfig {
	fn structure(self) -> Value {
		match self {
			GraphQLFunctionsConfig::None => Value::None,
			GraphQLFunctionsConfig::Auto => Value::String(Strand::new_static("AUTO")),
			GraphQLFunctionsConfig::Include(fs) => Value::from(map!(
				"include" => Value::Array(fs.into_iter().map(Value::from).collect()),
			)),
			GraphQLFunctionsConfig::Exclude(fs) => Value::from(map!(
				"exclude" => Value::Array(fs.into_iter().map(Value::from).collect()),
			)),
		}
	}
}

impl From<sql::statements::define::config::graphql::FunctionsConfig> for GraphQLFunctionsConfig {
	fn from(v: sql::statements::define::config::graphql::FunctionsConfig) -> Self {
		use sql::statements::define::config::graphql::FunctionsConfig;
		match v {
			FunctionsConfig::None => Self::None,
			FunctionsConfig::Auto => Self::Auto,
			FunctionsConfig::Include(cs) => Self::Include(cs),
			FunctionsConfig::Exclude(cs) => Self::Exclude(cs),
		}
	}
}

impl From<GraphQLFunctionsConfig> for sql::statements::define::config::graphql::FunctionsConfig {
	fn from(v: GraphQLFunctionsConfig) -> Self {
		match v {
			GraphQLFunctionsConfig::None => Self::None,
			GraphQLFunctionsConfig::Auto => Self::Auto,
			GraphQLFunctionsConfig::Include(cs) => Self::Include(cs),
			GraphQLFunctionsConfig::Exclude(cs) => Self::Exclude(cs),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum GraphQLIntrospectionConfig {
	#[default]
	Auto,
	None,
}

impl InfoStructure for GraphQLIntrospectionConfig {
	fn structure(self) -> Value {
		match self {
			GraphQLIntrospectionConfig::Auto => Value::String(Strand::new_static("AUTO")),
			GraphQLIntrospectionConfig::None => Value::None,
		}
	}
}

impl From<sql::statements::define::config::graphql::IntrospectionConfig>
	for GraphQLIntrospectionConfig
{
	fn from(v: sql::statements::define::config::graphql::IntrospectionConfig) -> Self {
		use sql::statements::define::config::graphql::IntrospectionConfig;
		match v {
			IntrospectionConfig::Auto => Self::Auto,
			IntrospectionConfig::None => Self::None,
		}
	}
}

impl From<GraphQLIntrospectionConfig>
	for sql::statements::define::config::graphql::IntrospectionConfig
{
	fn from(v: GraphQLIntrospectionConfig) -> Self {
		match v {
			GraphQLIntrospectionConfig::Auto => Self::Auto,
			GraphQLIntrospectionConfig::None => Self::None,
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct DefaultConfig {
	pub namespace: Option<String>,
	pub database: Option<String>,
}

impl ToSql for DefaultConfig {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "DEFAULT");
		if let Some(namespace) = &self.namespace {
			write_sql!(f, fmt, " NAMESPACE {}", EscapeKwFreeIdent(namespace));
		}
		if let Some(database) = &self.database {
			write_sql!(f, fmt, " DATABASE {}", EscapeKwFreeIdent(database));
		}
	}
}

impl InfoStructure for DefaultConfig {
	fn structure(self) -> Value {
		Value::from(map!(
			"namespace", if let Some(x) = self.namespace => Value::String(x.into()),
			"database", if let Some(x) = self.database => Value::String(x.into()),
		))
	}
}

/// Runtime form of [`StoredConfigDefinition`]: the API config's permissions parsed;
/// the GraphQL and default configs are plain data and carry over unchanged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ConfigDefinition {
	Default(DefaultConfig),
	GraphQL(GraphQLConfig),
	Api(ApiConfig),
}

impl FromStored for ConfigDefinition {
	type Stored = StoredConfigDefinition;

	fn from_stored(stored: &StoredConfigDefinition) -> anyhow::Result<ConfigDefinition> {
		fn build(stored: &StoredConfigDefinition) -> anyhow::Result<ConfigDefinition> {
			Ok(match stored {
				StoredConfigDefinition::Default(c) => ConfigDefinition::Default(c.clone()),
				StoredConfigDefinition::GraphQL(c) => ConfigDefinition::GraphQL(c.clone()),
				StoredConfigDefinition::Api(c) => ConfigDefinition::Api(ApiConfig::from_stored(c)?),
			})
		}
		build(stored).with_context(|| {
			format!(
				"the stored `{}` config definition no longer compiles",
				match stored {
					StoredConfigDefinition::Default(_) => "default",
					StoredConfigDefinition::GraphQL(_) => "GraphQL",
					StoredConfigDefinition::Api(_) => "API",
				}
			)
		})
	}
}

impl ConfigDefinition {
	/// See [`StoredConfigDefinition::try_into_graphql`]; same conversion on
	/// the compiled form. Only the GraphQL schema builder consumes this, so it
	/// is gated to the same feature to stay dead-code-free on builds without it.
	#[cfg(feature = "graphql")]
	pub(crate) fn try_into_graphql(self) -> anyhow::Result<GraphQLConfig> {
		match self {
			ConfigDefinition::GraphQL(g) => Ok(g),
			c => fail!(
				"found {} when a graphql config was expected",
				surrealdb_types::ToSql::to_sql(&c.to_stored())
			),
		}
	}

	/// Returns the API configuration, or an error naming the config kind
	/// actually found.
	pub(crate) fn try_as_api(&self) -> anyhow::Result<&ApiConfig> {
		match self {
			ConfigDefinition::Api(a) => Ok(a),
			c => fail!(
				"found {} when a api config was expected",
				surrealdb_types::ToSql::to_sql(&c.to_stored())
			),
		}
	}

	pub(crate) fn to_stored(&self) -> StoredConfigDefinition {
		match self {
			ConfigDefinition::Default(c) => StoredConfigDefinition::Default(c.clone()),
			ConfigDefinition::GraphQL(c) => StoredConfigDefinition::GraphQL(c.clone()),
			ConfigDefinition::Api(c) => StoredConfigDefinition::Api(c.to_stored()),
		}
	}
}
