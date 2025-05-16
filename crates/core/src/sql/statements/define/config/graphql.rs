use std::fmt::{self, Write};

use crate::sql::fmt::{pretty_indent, Fmt, Pretty};

use crate::sql::{Ident, Part, SqlValue};

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

impl From<GraphQLConfig> for crate::expr::statements::define::config::graphql::GraphQLConfig {
	fn from(v: GraphQLConfig) -> Self {
		crate::expr::statements::define::config::graphql::GraphQLConfig {
			tables: v.tables.into(),
			functions: v.functions.into(),
		}
	}
}

impl From<crate::expr::statements::define::config::graphql::GraphQLConfig> for GraphQLConfig {
	fn from(v: crate::expr::statements::define::config::graphql::GraphQLConfig) -> Self {
		GraphQLConfig {
			tables: v.tables.into(),
			functions: v.functions.into(),
		}
	}
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

impl From<TablesConfig> for crate::expr::statements::define::config::graphql::TablesConfig {
	fn from(v: TablesConfig) -> Self {
		match v {
			TablesConfig::None => Self::None,
			TablesConfig::Auto => Self::Auto,
			TablesConfig::Include(cs) => Self::Include(cs.into_iter().map(Into::into).collect()),
			TablesConfig::Exclude(cs) => Self::Exclude(cs.into_iter().map(Into::into).collect()),
		}
	}
}

impl From<crate::expr::statements::define::config::graphql::TablesConfig> for TablesConfig {
	fn from(v: crate::expr::statements::define::config::graphql::TablesConfig) -> Self {
		match v {
			crate::expr::statements::define::config::graphql::TablesConfig::None => Self::None,
			crate::expr::statements::define::config::graphql::TablesConfig::Auto => Self::Auto,
			crate::expr::statements::define::config::graphql::TablesConfig::Include(cs) => {
				Self::Include(cs.into_iter().map(Into::<TableConfig>::into).collect())
			}
			crate::expr::statements::define::config::graphql::TablesConfig::Exclude(cs) => {
				Self::Exclude(cs.into_iter().map(Into::<TableConfig>::into).collect())
			}
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct TableConfig {
	pub name: String,
}

impl From<TableConfig> for crate::expr::statements::define::config::graphql::TableConfig {
	fn from(v: TableConfig) -> Self {
		crate::expr::statements::define::config::graphql::TableConfig {
			name: v.name.into(),
		}
	}
}
impl From<crate::expr::statements::define::config::graphql::TableConfig> for TableConfig {
	fn from(v: crate::expr::statements::define::config::graphql::TableConfig) -> Self {
		TableConfig {
			name: v.name.into(),
		}
	}
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

impl From<FunctionsConfig> for crate::expr::statements::define::config::graphql::FunctionsConfig {
	fn from(v: FunctionsConfig) -> Self {
		match v {
			FunctionsConfig::None => Self::None,
			FunctionsConfig::Auto => Self::Auto,
			FunctionsConfig::Include(cs) => Self::Include(cs.into_iter().map(Into::into).collect()),
			FunctionsConfig::Exclude(cs) => Self::Exclude(cs.into_iter().map(Into::into).collect()),
		}
	}
}

impl From<crate::expr::statements::define::config::graphql::FunctionsConfig> for FunctionsConfig {
	fn from(v: crate::expr::statements::define::config::graphql::FunctionsConfig) -> Self {
		match v {
			crate::expr::statements::define::config::graphql::FunctionsConfig::None => Self::None,
			crate::expr::statements::define::config::graphql::FunctionsConfig::Auto => Self::Auto,
			crate::expr::statements::define::config::graphql::FunctionsConfig::Include(cs) => {
				Self::Include(cs.into_iter().map(Into::<Ident>::into).collect())
			}
			crate::expr::statements::define::config::graphql::FunctionsConfig::Exclude(cs) => {
				Self::Exclude(cs.into_iter().map(Into::<Ident>::into).collect())
			}
		}
	}
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

pub fn val_to_ident(val: SqlValue) -> Result<Ident, SqlValue> {
	match val {
		SqlValue::Strand(s) => Ok(s.0.into()),
		SqlValue::Table(n) => Ok(n.0.into()),
		SqlValue::Idiom(ref i) => match &i[..] {
			[Part::Field(n)] => Ok(n.to_raw().into()),
			_ => Err(val),
		},
		_ => Err(val),
	}
}

impl TryFrom<SqlValue> for TableConfig {
	type Error = SqlValue;

	fn try_from(value: SqlValue) -> Result<Self, Self::Error> {
		match value {
			v @ SqlValue::Strand(_) | v @ SqlValue::Table(_) | v @ SqlValue::Idiom(_) => {
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
