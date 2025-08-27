use std::fmt::{self, Display, Write};

use crate::sql::Ident;
use crate::sql::fmt::{Fmt, Pretty, pretty_indent};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct TableConfig {
	pub name: String,
}

impl From<TableConfig> for crate::expr::statements::define::config::graphql::TableConfig {
	fn from(v: TableConfig) -> Self {
		crate::expr::statements::define::config::graphql::TableConfig {
			name: v.name,
		}
	}
}
impl From<crate::expr::statements::define::config::graphql::TableConfig> for TableConfig {
	fn from(v: crate::expr::statements::define::config::graphql::TableConfig) -> Self {
		TableConfig {
			name: v.name,
		}
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
