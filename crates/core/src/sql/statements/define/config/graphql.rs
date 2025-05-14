use std::fmt::{self, Write};

use crate::sql::fmt::{pretty_indent, Fmt, Pretty};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Ident, Part, Value};

use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct GraphQLConfig {
	pub tables: TablesConfig,
	pub functions: FunctionsConfig,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
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
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct TableConfig {
	pub name: String,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum FunctionsConfig {
	#[default]
	None,
	Auto,
	Include(Vec<Ident>),
	Exclude(Vec<Ident>),
}

crate::sql::impl_display_from_sql!(GraphQLConfig);

impl crate::sql::DisplaySql for GraphQLConfig {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, " GRAPHQL")?;

		write!(f, " TABLES {}", self.tables)?;
		write!(f, " FUNCTIONS {}", self.functions)?;
		Ok(())
	}
}

crate::sql::impl_display_from_sql!(TablesConfig);

impl crate::sql::DisplaySql for TablesConfig {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
			TablesConfig::Exclude(_) => todo!(),
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

pub fn val_to_ident(val: Value) -> Result<Ident, Value> {
	match val {
		Value::Strand(s) => Ok(s.0.into()),
		Value::Table(n) => Ok(n.0.into()),
		Value::Idiom(ref i) => match &i[..] {
			[Part::Field(n)] => Ok(n.to_raw().into()),
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
}

crate::sql::impl_display_from_sql!(TableConfig);

impl crate::sql::DisplaySql for TableConfig {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.name)?;
		Ok(())
	}
}

crate::sql::impl_display_from_sql!(FunctionsConfig);

impl crate::sql::DisplaySql for FunctionsConfig {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
				"include" => Value::Array(fs.into_iter().map(|i| Value::from(i.to_raw())).collect()),
			)),
			FunctionsConfig::Exclude(fs) => Value::from(map!(
				"exclude" => Value::Array(fs.into_iter().map(|i| Value::from(i.to_raw())).collect()),
			)),
		}
	}
}
