use std::fmt::{self, Display, Write};

use anyhow::Result;
use revision::revisioned;

use crate::catalog::ApiConfigDefinition;
use crate::expr::fmt::{Fmt, Pretty, pretty_indent};
use crate::expr::statements::info::InfoStructure;
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
	pub fn name(&self) -> String {
		match self {
			ConfigDefinition::GraphQL(_) => ConfigKind::GraphQL.to_string(),
			ConfigDefinition::Api(_) => ConfigKind::Api.to_string(),
		}
	}

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
	pub tables: TablesConfig,
	pub functions: FunctionsConfig,
}

impl Display for GraphQLConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, " GRAPHQL")?;

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
pub enum TablesConfig {
	#[default]
	None,
	Auto,
	Include(Vec<TableConfig>),
	Exclude(Vec<TableConfig>),
}

impl Display for TablesConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			TablesConfig::Auto => write!(f, "AUTO")?,
			TablesConfig::None => write!(f, "NONE")?,
			TablesConfig::Include(cs) => {
				let mut f = Pretty::from(f);
				write!(f, "INCLUDE ")?;
				if !cs.is_empty() {
					let indent = pretty_indent();
					write!(f, "{}", Fmt::pretty_comma_separated(cs.as_slice()))?;
					drop(indent);
				}
			}
			TablesConfig::Exclude(cs) => {
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

impl InfoStructure for TablesConfig {
	fn structure(self) -> Value {
		match self {
			TablesConfig::None => Value::None,
			TablesConfig::Auto => Value::Strand("AUTO".into()),
			TablesConfig::Include(ts) => Value::from(map!(
				"include" => Value::Array(ts.into_iter().map(InfoStructure::structure).collect()),
			)),
			TablesConfig::Exclude(ts) => Value::from(map!(
				"exclude" => Value::Array(ts.into_iter().map(InfoStructure::structure).collect()),
			)),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TableConfig {
	pub name: String,
}

impl From<String> for TableConfig {
	fn from(value: String) -> Self {
		Self {
			name: value,
		}
	}
}

impl Display for TableConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.name)?;
		Ok(())
	}
}

impl InfoStructure for TableConfig {
	fn structure(self) -> Value {
		Value::from(map!(
			"name" => Value::from(self.name),
		))
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum FunctionsConfig {
	#[default]
	None,
	Auto,
	Include(Vec<String>),
	Exclude(Vec<String>),
}

impl Display for FunctionsConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			FunctionsConfig::Auto => write!(f, "AUTO")?,
			FunctionsConfig::None => write!(f, "NONE")?,
			FunctionsConfig::Include(cs) => {
				let mut f = Pretty::from(f);
				write!(f, "INCLUDE [")?;
				if !cs.is_empty() {
					let indent = pretty_indent();
					write!(f, "{}", Fmt::pretty_comma_separated(cs.as_slice()))?;
					drop(indent);
				}
				f.write_char(']')?;
			}
			FunctionsConfig::Exclude(cs) => {
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

impl InfoStructure for FunctionsConfig {
	fn structure(self) -> Value {
		match self {
			FunctionsConfig::None => Value::None,
			FunctionsConfig::Auto => Value::Strand("AUTO".into()),
			FunctionsConfig::Include(fs) => Value::from(map!(
				"include" => Value::Array(fs.into_iter().map(Value::from).collect()),
			)),
			FunctionsConfig::Exclude(fs) => Value::from(map!(
				"exclude" => Value::Array(fs.into_iter().map(Value::from).collect()),
			)),
		}
	}
}
