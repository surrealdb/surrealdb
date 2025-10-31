use std::fmt::{self, Display, Write};

use anyhow::Result;
use revision::revisioned;

use crate::catalog::ApiConfigDefinition;
use crate::expr::statements::info::InfoStructure;
use crate::fmt::{Fmt, Pretty, pretty_indent};
use crate::iam::ConfigKind;
use crate::kvs::impl_kv_value_revisioned;
use crate::val::Value;

/// The config struct as it is stored on disk.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ConfigDefinition {
	GraphQL(GraphQLConfig),
	Api(ApiConfigDefinition),
}
impl_kv_value_revisioned!(ConfigDefinition);

impl ConfigDefinition {
	/// Get the name of the config.
	pub fn name(&self) -> String {
		match self {
			ConfigDefinition::GraphQL(_) => ConfigKind::GraphQL.to_string(),
			ConfigDefinition::Api(_) => ConfigKind::Api.to_string(),
		}
	}

	/// Convert the config definition into a graphql config.
	#[allow(unused)]
	pub fn try_into_graphql(self) -> Result<GraphQLConfig> {
		match self {
			ConfigDefinition::GraphQL(g) => Ok(g),
			c => fail!("found {c} when a graphql config was expected"),
		}
	}

	pub fn try_as_api(&self) -> Result<&ApiConfigDefinition> {
		match self {
			ConfigDefinition::Api(a) => Ok(a),
			c => fail!("found {c} when a api config was expected"),
		}
	}
}

impl Display for ConfigDefinition {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match &self {
			ConfigDefinition::GraphQL(v) => Display::fmt(v, f),
			ConfigDefinition::Api(v) => Display::fmt(v, f),
		}
	}
}

impl InfoStructure for ConfigDefinition {
	fn structure(self) -> Value {
		match self {
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

impl Display for GraphQLConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "GRAPHQL")?;

		write!(f, " TABLES {}", self.tables)?;
		write!(f, " FUNCTIONS {}", self.functions)?;
		Ok(())
	}
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

impl Display for GraphQLTablesConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			GraphQLTablesConfig::Auto => write!(f, "AUTO")?,
			GraphQLTablesConfig::None => write!(f, "NONE")?,
			GraphQLTablesConfig::Include(cs) => {
				let mut f = Pretty::from(f);
				write!(f, "INCLUDE ")?;
				if !cs.is_empty() {
					let indent = pretty_indent();
					write!(f, "{}", Fmt::pretty_comma_separated(cs.as_slice()))?;
					drop(indent);
				}
			}
			GraphQLTablesConfig::Exclude(cs) => {
				let mut f = Pretty::from(f);
				write!(f, "EXCLUDE")?;
				if !cs.is_empty() {
					let indent = pretty_indent();
					write!(f, "{}", Fmt::pretty_comma_separated(cs.as_slice()))?;
					drop(indent);
				}
			}
		}

		Ok(())
	}
}

impl InfoStructure for GraphQLTablesConfig {
	fn structure(self) -> Value {
		match self {
			GraphQLTablesConfig::None => Value::None,
			GraphQLTablesConfig::Auto => Value::String("AUTO".into()),
			GraphQLTablesConfig::Include(ts) => Value::from(map!(
				"include" => Value::Array(ts.into_iter().map(Value::String).collect()),
			)),
			GraphQLTablesConfig::Exclude(ts) => Value::from(map!(
				"exclude" => Value::Array(ts.into_iter().map(Value::String).collect()),
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

impl Display for GraphQLFunctionsConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			GraphQLFunctionsConfig::Auto => write!(f, "AUTO")?,
			GraphQLFunctionsConfig::None => write!(f, "NONE")?,
			GraphQLFunctionsConfig::Include(cs) => {
				let mut f = Pretty::from(f);
				write!(f, "INCLUDE [")?;
				if !cs.is_empty() {
					let indent = pretty_indent();
					write!(f, "{}", Fmt::pretty_comma_separated(cs.as_slice()))?;
					drop(indent);
				}
				f.write_char(']')?;
			}
			GraphQLFunctionsConfig::Exclude(cs) => {
				let mut f = Pretty::from(f);
				write!(f, "EXCLUDE [")?;
				if !cs.is_empty() {
					let indent = pretty_indent();
					write!(f, "{}", Fmt::pretty_comma_separated(cs.as_slice()))?;
					drop(indent);
				}
				f.write_char(']')?;
			}
		}

		Ok(())
	}
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
