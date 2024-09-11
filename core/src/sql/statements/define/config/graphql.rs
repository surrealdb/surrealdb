use std::fmt::{self, Display, Write};

use crate::sql::fmt::{pretty_indent, Fmt, Pretty};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Ident, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct GraphQLConfig {
	pub tables: TablesConfig,
	pub functions: FunctionsConfig,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum TablesConfig {
	#[default]
	None,
	Auto,
	Include(Vec<TableConfig>),
	Exclude(Vec<TableConfig>),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct TableConfig {
	name: String,
	fields: Vec<String>,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum FunctionsConfig {
	#[default]
	None,
	Auto,
	Include(Vec<Ident>),
	Exclude(Vec<Ident>),
}

impl Display for GraphQLConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, " GRAPHQL")?;

		write!(f, " TABLES {}", self.tables)?;
		write!(f, " FUNCTIONS {}", self.functions)?;
		Ok(())
	}
}

impl Display for TablesConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			TablesConfig::Auto => write!(f, "AUTO")?,
			TablesConfig::None => write!(f, "NONE")?,
			TablesConfig::Include(cs) => {
				let mut f = Pretty::from(f);
				write!(f, "INCLUDE [")?;
				if !cs.is_empty() {
					let indent = pretty_indent();
					write!(f, "{}", Fmt::pretty_comma_separated(cs.as_slice()))?;
					write!(f, "todo")?;
					drop(indent);
				}
				f.write_char(']')?;
			}
			TablesConfig::Exclude(_) => todo!(),
		}

		Ok(())
	}
}

impl Display for TableConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "todo")?;
		Ok(())
	}
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

impl InfoStructure for GraphQLConfig {
	fn structure(self) -> Value {
		Value::from(map!(
			"tables" => self.tables.structure(),
			"functions" => self.functions.structure(),
		))
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

impl InfoStructure for TableConfig {
	fn structure(self) -> Value {
		Value::from(map!(
			"name" => Value::from(self.name),
			"fields" => Value::Array(self.fields.into_iter().map(Value::from).collect())
		))
	}
}

impl InfoStructure for FunctionsConfig {
	fn structure(self) -> Value {
		match self {
			FunctionsConfig::None => Value::None,
			FunctionsConfig::Auto => Value::Strand("AUTO".into()),
			FunctionsConfig::Include(fs) => Value::from(map!(
				"include" => Value::Array(fs.into_iter().map(|i| Value::from(i.to_raw())).collect()),
			)),
			FunctionsConfig::Exclude(fs) => Value::from(map!(
				"exclude" => Value::Array(fs.into_iter().map(|i| Value::from(i.to_raw())).collect()),
			)),
		}
	}
}
