use std::fmt::{self, Display, Write};

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::expr::fmt::{Fmt, Pretty, pretty_indent};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Ident, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct GraphQLConfig {
	pub tables: TablesConfig,
	pub functions: FunctionsConfig,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum TablesConfig {
	#[default]
	None,
	Auto,
	Include(Vec<TableConfig>),
	Exclude(Vec<TableConfig>),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct TableConfig {
	pub name: String,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
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

impl From<String> for TableConfig {
	fn from(value: String) -> Self {
		Self {
			name: value,
		}
	}
}

/*
pub fn val_to_ident(val: Value) -> Result<Ident, Value> {
	match val {
		Value::Strand(s) => Ok(Ident::from_strand(s)),
		Value::Table(n) => Ok(Ident::from_strand(n)),
		Value::Idiom(ref i) => match &i[..] {
			[Part::Field(n)] => Ok(n.clone()),
			_ => Err(val),
		},
		_ => Err(val),
	}
}

impl TryFrom<Value> for TableConfig {
	type Error = Value;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			v @ Value::Strand(_) | v @ Value::Table(_) | v @ Value::Idiom(_) => {
				val_to_ident(v).map(|i| i.0.into())
			}
			_ => Err(value),
		}
	}
}*/

impl Display for TableConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.name)?;
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
		))
	}
}

impl InfoStructure for FunctionsConfig {
	fn structure(self) -> Value {
		match self {
			FunctionsConfig::None => Value::None,
			FunctionsConfig::Auto => Value::Strand("AUTO".into()),
			FunctionsConfig::Include(fs) => Value::from(map!(
				"include" => Value::Array(fs.into_iter().map(|i| Value::from(i.as_raw_string())).collect()),
			)),
			FunctionsConfig::Exclude(fs) => Value::from(map!(
				"exclude" => Value::Array(fs.into_iter().map(|i| Value::from(i.as_raw_string())).collect()),
			)),
		}
	}
}
