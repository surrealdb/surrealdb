use anyhow::Result;
use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::catalog::ApiConfigDefinition;
use crate::expr::statements::info::InfoStructure;
use crate::fmt::EscapeKwFreeIdent;
use crate::iam::ConfigKind;
use crate::kvs::impl_kv_value_revisioned;
use crate::val::{TableName, Value};

/// The config struct as it is stored on disk.
#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ConfigDefinition {
	Default(DefaultConfig),
	GraphQL(GraphQLConfig),
	Api(ApiConfigDefinition),
	#[revision(start = 2)]
	Ai(AiConfig),
}
impl_kv_value_revisioned!(ConfigDefinition);

impl ConfigDefinition {
	/// Get the name of the config.
	pub fn name(&self) -> String {
		match self {
			ConfigDefinition::Default(_) => ConfigKind::Default.to_string(),
			ConfigDefinition::GraphQL(_) => ConfigKind::GraphQL.to_string(),
			ConfigDefinition::Api(_) => ConfigKind::Api.to_string(),
			ConfigDefinition::Ai(_) => ConfigKind::Ai.to_string(),
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

	pub fn try_as_ai(&self) -> Result<&AiConfig> {
		match self {
			ConfigDefinition::Ai(a) => Ok(a),
			c => fail!("found {} when an ai config was expected", c.to_sql()),
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
			ConfigDefinition::Ai(v) => {
				let sql_config: crate::sql::statements::define::config::ai::AiConfig =
					v.clone().into();
				sql_config.fmt_sql(f, fmt)
			}
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
			ConfigDefinition::Ai(v) => Value::from(map!(
				"ai" => v.structure()
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
			GraphQLTablesConfig::Auto => Value::String("AUTO".into()),
			GraphQLTablesConfig::Include(ts) => Value::from(map!(
				"include" => Value::Array(ts.into_iter().map(Value::Table).collect()),
			)),
			GraphQLTablesConfig::Exclude(ts) => Value::from(map!(
				"exclude" => Value::Array(ts.into_iter().map(Value::Table).collect()),
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
pub enum GraphQLIntrospectionConfig {
	#[default]
	Auto,
	None,
}

impl InfoStructure for GraphQLIntrospectionConfig {
	fn structure(self) -> Value {
		match self {
			GraphQLIntrospectionConfig::Auto => Value::String("AUTO".into()),
			GraphQLIntrospectionConfig::None => Value::None,
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
			"namespace", if let Some(x) = self.namespace => Value::String(x),
			"database", if let Some(x) = self.database => Value::String(x),
		))
	}
}

/// AI provider API keys and base URLs for a database.
///
/// Stored via DEFINE CONFIG AI ON DATABASE. When set, these override
/// the corresponding environment variables for ai::embed, ai::generate, ai::chat.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct AiConfig {
	pub openai_api_key: Option<String>,
	pub openai_base_url: Option<String>,
	pub anthropic_api_key: Option<String>,
	pub anthropic_base_url: Option<String>,
	pub google_api_key: Option<String>,
	pub google_base_url: Option<String>,
	pub voyage_api_key: Option<String>,
	pub voyage_base_url: Option<String>,
	pub huggingface_api_key: Option<String>,
	pub huggingface_base_url: Option<String>,
}

impl ToSql for AiConfig {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "AI ON DATABASE");
		let escape = |s: &str| s.replace('\'', "''");
		if let Some(v) = &self.openai_api_key {
			write_sql!(f, fmt, " OPENAI_API_KEY '{}'", escape(v));
		}
		if let Some(v) = &self.openai_base_url {
			write_sql!(f, fmt, " OPENAI_BASE_URL '{}'", escape(v));
		}
		if let Some(v) = &self.anthropic_api_key {
			write_sql!(f, fmt, " ANTHROPIC_API_KEY '{}'", escape(v));
		}
		if let Some(v) = &self.anthropic_base_url {
			write_sql!(f, fmt, " ANTHROPIC_BASE_URL '{}'", escape(v));
		}
		if let Some(v) = &self.google_api_key {
			write_sql!(f, fmt, " GOOGLE_API_KEY '{}'", escape(v));
		}
		if let Some(v) = &self.google_base_url {
			write_sql!(f, fmt, " GOOGLE_BASE_URL '{}'", escape(v));
		}
		if let Some(v) = &self.voyage_api_key {
			write_sql!(f, fmt, " VOYAGE_API_KEY '{}'", escape(v));
		}
		if let Some(v) = &self.voyage_base_url {
			write_sql!(f, fmt, " VOYAGE_BASE_URL '{}'", escape(v));
		}
		if let Some(v) = &self.huggingface_api_key {
			write_sql!(f, fmt, " HUGGINGFACE_API_KEY '{}'", escape(v));
		}
		if let Some(v) = &self.huggingface_base_url {
			write_sql!(f, fmt, " HUGGINGFACE_BASE_URL '{}'", escape(v));
		}
	}
}

impl InfoStructure for AiConfig {
	fn structure(self) -> Value {
		Value::from(map!(
			"openai_api_key", if let Some(x) = self.openai_api_key => Value::String(x),
			"openai_base_url", if let Some(x) = self.openai_base_url => Value::String(x),
			"anthropic_api_key", if let Some(x) = self.anthropic_api_key => Value::String(x),
			"anthropic_base_url", if let Some(x) = self.anthropic_base_url => Value::String(x),
			"google_api_key", if let Some(x) = self.google_api_key => Value::String(x),
			"google_base_url", if let Some(x) = self.google_base_url => Value::String(x),
			"voyage_api_key", if let Some(x) = self.voyage_api_key => Value::String(x),
			"voyage_base_url", if let Some(x) = self.voyage_base_url => Value::String(x),
			"huggingface_api_key", if let Some(x) = self.huggingface_api_key => Value::String(x),
			"huggingface_base_url", if let Some(x) = self.huggingface_base_url => Value::String(x),
		))
	}
}
