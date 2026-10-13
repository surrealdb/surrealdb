//! GraphQL exposure configuration.
//!
//! `GraphQLConfig` selects which tables and functions a database exposes over
//! GraphQL. It is shared vocabulary between the DEFINE CONFIG statement and
//! the catalog's config definition; both planes persist it, so the revisioned
//! shapes here are storage-stable.

use revision::revisioned;
use surrealdb_strand::Strand;

use crate::expr::statements::info::InfoStructure;
use crate::sql;
use crate::val::{TableName, Value};

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
		match v {
			sql::statements::define::config::graphql::IntrospectionConfig::Auto => Self::Auto,
			sql::statements::define::config::graphql::IntrospectionConfig::None => Self::None,
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
