use surrealdb_types::{SqlFormat, ToSql};

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
	Include(Vec<TableConfig>),
	Exclude(Vec<TableConfig>),
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
	Include(Vec<String>),
	Exclude(Vec<String>),
}

impl ToSql for GraphQLConfig {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("GRAPHQL");
		f.push_str(" TABLES ");
		self.tables.fmt_sql(f, fmt);
		f.push_str(" FUNCTIONS ");
		self.functions.fmt_sql(f, fmt);
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

impl ToSql for TablesConfig {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			TablesConfig::Auto => f.push_str("AUTO"),
			TablesConfig::None => f.push_str("NONE"),
			TablesConfig::Include(cs) => {
				f.push_str("INCLUDE ");
				for (i, table) in cs.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					table.fmt_sql(f, fmt);
				}
			}
			TablesConfig::Exclude(cs) => {
				f.push_str("EXCLUDE ");
				for (i, table) in cs.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					table.fmt_sql(f, fmt);
				}
			}
		}
	}
}

impl ToSql for TableConfig {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.name.fmt_sql(f, fmt);
	}
}

impl ToSql for FunctionsConfig {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			FunctionsConfig::Auto => f.push_str("AUTO"),
			FunctionsConfig::None => f.push_str("NONE"),
			FunctionsConfig::Include(cs) => {
				f.push_str("INCLUDE [");
				for (i, func) in cs.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					func.fmt_sql(f, fmt);
				}
				f.push(']');
			}
			FunctionsConfig::Exclude(cs) => {
				f.push_str("EXCLUDE [");
				for (i, func) in cs.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					func.fmt_sql(f, fmt);
				}
				f.push(']');
			}
		}
	}
}
