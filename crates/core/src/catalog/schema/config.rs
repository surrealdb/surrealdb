use anyhow::Result;
use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::catalog::ApiConfigDefinition;
use crate::expr::statements::info::InfoStructure;
use crate::fmt::EscapeKwFreeIdent;
use crate::iam::ConfigKind;
use crate::kvs::impl_kv_value_revisioned;
use crate::val::Value;

/// The config struct as it is stored on disk.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ConfigDefinition {
	Default(DefaultConfig),
	GraphQL(GraphQLConfig),
	Api(ApiConfigDefinition),
}
impl_kv_value_revisioned!(ConfigDefinition);

impl ConfigDefinition {
	/// Get the name of the config.
	pub fn name(&self) -> String {
		match self {
			ConfigDefinition::Default(_) => ConfigKind::Default.to_string(),
			ConfigDefinition::GraphQL(_) => ConfigKind::GraphQL.to_string(),
			ConfigDefinition::Api(_) => ConfigKind::Api.to_string(),
		}
	}

	/// Convert the config definition into a graphql config.
	#[allow(unused)]
	pub fn try_into_graphql(self) -> Result<GraphQLConfig> {
		match self {
			ConfigDefinition::GraphQL(g) => Ok(g),
			c => fail!("found {} when a graphql config was expected", c.to_sql()),
		}
	}

	pub fn try_as_api(&self) -> Result<&ApiConfigDefinition> {
		match self {
			ConfigDefinition::Api(a) => Ok(a),
			c => fail!("found {} when a api config was expected", c.to_sql()),
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct GraphQLConfig {
	pub tables: GraphQLTablesConfig,
	pub functions: GraphQLFunctionsConfig,
}

impl InfoStructure for GraphQLConfig {
	fn structure(self) -> Value {
		Value::from(map!(
			"tables" => self.tables.structure(),
			"functions" => self.functions.structure(),
		))
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum GraphQLTablesConfig {
	#[default]
	None,
	Auto,
	Include(Vec<String>),
	Exclude(Vec<String>),
}

impl InfoStructure for GraphQLTablesConfig {
	fn structure(self) -> Value {
		match self {
			GraphQLTablesConfig::None => Value::None,
			GraphQLTablesConfig::Auto => Value::String("AUTO".into()),
			GraphQLTablesConfig::Include(ts) => Value::from(map!(
				"include" => Value::Array(ts.into_iter().map(|v| Value::String(v.into())).collect()),
			)),
			GraphQLTablesConfig::Exclude(ts) => Value::from(map!(
				"exclude" => Value::Array(ts.into_iter().map(|v| Value::String(v.into())).collect()),
			)),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum GraphQLFunctionsConfig {
	#[default]
	None,
	Auto,
	Include(Vec<String>),
	Exclude(Vec<String>),
}

impl InfoStructure for GraphQLFunctionsConfig {
	fn structure(self) -> Value {
		match self {
			GraphQLFunctionsConfig::None => Value::None,
			GraphQLFunctionsConfig::Auto => Value::String("AUTO".into()),
			GraphQLFunctionsConfig::Include(fs) => Value::from(map!(
				"include" => Value::Array(fs.into_iter().map(Value::from).collect()),
			)),
			GraphQLFunctionsConfig::Exclude(fs) => Value::from(map!(
				"exclude" => Value::Array(fs.into_iter().map(Value::from).collect()),
			)),
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
