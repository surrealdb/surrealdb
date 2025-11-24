use std::fmt::{self, Display, Write};

use crate::fmt::{EscapeKwFreeIdent, Fmt, Pretty, pretty_indent};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct GraphQLConfig {
	pub tables: TablesConfig,
	pub functions: FunctionsConfig,
}

impl From<GraphQLConfig> for crate::catalog::GraphQLConfig {
	fn from(v: GraphQLConfig) -> Self {
		crate::catalog::GraphQLConfig {
			tables: v.tables.into(),
			functions: v.functions.into(),
		}
	}
}

impl From<crate::catalog::GraphQLConfig> for GraphQLConfig {
	fn from(v: crate::catalog::GraphQLConfig) -> Self {
		GraphQLConfig {
			tables: v.tables.into(),
			functions: v.functions.into(),
		}
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum TablesConfig {
	#[default]
	None,
	Auto,
	Include(
		#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
		Vec<TableConfig>,
	),
	Exclude(
		#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
		Vec<TableConfig>,
	),
}

impl From<TablesConfig> for crate::catalog::GraphQLTablesConfig {
	fn from(v: TablesConfig) -> Self {
		match v {
			TablesConfig::None => Self::None,
			TablesConfig::Auto => Self::Auto,
			TablesConfig::Include(cs) => Self::Include(cs.into_iter().map(|t| t.name).collect()),
			TablesConfig::Exclude(cs) => Self::Exclude(cs.into_iter().map(|t| t.name).collect()),
		}
	}
}

impl From<crate::catalog::GraphQLTablesConfig> for TablesConfig {
	fn from(v: crate::catalog::GraphQLTablesConfig) -> Self {
		match v {
			crate::catalog::GraphQLTablesConfig::None => Self::None,
			crate::catalog::GraphQLTablesConfig::Auto => Self::Auto,
			crate::catalog::GraphQLTablesConfig::Include(cs) => Self::Include(
				cs.into_iter()
					.map(|t| TableConfig {
						name: t,
					})
					.collect(),
			),
			crate::catalog::GraphQLTablesConfig::Exclude(cs) => Self::Exclude(
				cs.into_iter()
					.map(|t| TableConfig {
						name: t,
					})
					.collect(),
			),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct TableConfig {
	pub name: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum FunctionsConfig {
	#[default]
	None,
	Auto,
	// These variants are not actually implemented yet
	#[cfg_attr(feature = "arbitrary", arbitrary(skip))]
	Include(Vec<String>),
	#[cfg_attr(feature = "arbitrary", arbitrary(skip))]
	Exclude(Vec<String>),
}

impl Display for GraphQLConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "GRAPHQL")?;

		write!(f, " TABLES {}", self.tables)?;
		write!(f, " FUNCTIONS {}", self.functions)?;
		Ok(())
	}
}

impl From<FunctionsConfig> for crate::catalog::GraphQLFunctionsConfig {
	fn from(v: FunctionsConfig) -> Self {
		match v {
			FunctionsConfig::None => Self::None,
			FunctionsConfig::Auto => Self::Auto,
			FunctionsConfig::Include(cs) => Self::Include(cs),
			FunctionsConfig::Exclude(cs) => Self::Exclude(cs),
		}
	}
}

impl From<crate::catalog::GraphQLFunctionsConfig> for FunctionsConfig {
	fn from(v: crate::catalog::GraphQLFunctionsConfig) -> Self {
		match v {
			crate::catalog::GraphQLFunctionsConfig::None => Self::None,
			crate::catalog::GraphQLFunctionsConfig::Auto => Self::Auto,
			crate::catalog::GraphQLFunctionsConfig::Include(cs) => Self::Include(cs),
			crate::catalog::GraphQLFunctionsConfig::Exclude(cs) => Self::Exclude(cs),
		}
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
				write!(f, "EXCLUDE ")?;
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

impl Display for TableConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", EscapeKwFreeIdent(&self.name))?;
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
