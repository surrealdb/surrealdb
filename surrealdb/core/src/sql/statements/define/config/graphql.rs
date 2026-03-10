use surrealdb_types::{SqlFormat, ToSql};

use crate::fmt::EscapeKwFreeIdent;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct GraphQLConfig {
	pub tables: TablesConfig,
	pub functions: FunctionsConfig,
	pub depth_limit: Option<u32>,
	pub complexity_limit: Option<u32>,
	pub introspection: IntrospectionConfig,
}

impl From<GraphQLConfig> for crate::catalog::GraphQLConfig {
	fn from(v: GraphQLConfig) -> Self {
		crate::catalog::GraphQLConfig {
			tables: v.tables.into(),
			functions: v.functions.into(),
			depth_limit: v.depth_limit,
			complexity_limit: v.complexity_limit,
			introspection: v.introspection.into(),
		}
	}
}

impl From<crate::catalog::GraphQLConfig> for GraphQLConfig {
	fn from(v: crate::catalog::GraphQLConfig) -> Self {
		GraphQLConfig {
			tables: v.tables.into(),
			functions: v.functions.into(),
			depth_limit: v.depth_limit,
			complexity_limit: v.complexity_limit,
			introspection: v.introspection.into(),
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
			TablesConfig::Include(cs) => {
				Self::Include(cs.into_iter().map(|t| t.name.into()).collect())
			}
			TablesConfig::Exclude(cs) => {
				Self::Exclude(cs.into_iter().map(|t| t.name.into()).collect())
			}
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
						name: t.into_string(),
					})
					.collect(),
			),
			crate::catalog::GraphQLTablesConfig::Exclude(cs) => Self::Exclude(
				cs.into_iter()
					.map(|t| TableConfig {
						name: t.into_string(),
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

/// Controls whether GraphQL schema introspection is enabled.
///
/// When set to `None`, introspection queries (`__schema`, `__type`, etc.) are disabled,
/// preventing clients from discovering the schema structure. This is useful in production
/// to avoid leaking table/field names to unauthorized users.
///
/// Defaults to `Auto` (introspection enabled).
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum IntrospectionConfig {
	#[default]
	Auto,
	None,
}

impl From<IntrospectionConfig> for crate::catalog::GraphQLIntrospectionConfig {
	fn from(v: IntrospectionConfig) -> Self {
		match v {
			IntrospectionConfig::Auto => Self::Auto,
			IntrospectionConfig::None => Self::None,
		}
	}
}

impl From<crate::catalog::GraphQLIntrospectionConfig> for IntrospectionConfig {
	fn from(v: crate::catalog::GraphQLIntrospectionConfig) -> Self {
		match v {
			crate::catalog::GraphQLIntrospectionConfig::Auto => Self::Auto,
			crate::catalog::GraphQLIntrospectionConfig::None => Self::None,
		}
	}
}

impl ToSql for IntrospectionConfig {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self {
			IntrospectionConfig::Auto => f.push_str("AUTO"),
			IntrospectionConfig::None => f.push_str("NONE"),
		}
	}
}

impl ToSql for GraphQLConfig {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("GRAPHQL");
		f.push_str(" TABLES ");
		self.tables.fmt_sql(f, fmt);
		f.push_str(" FUNCTIONS ");
		self.functions.fmt_sql(f, fmt);
		if let Some(depth) = self.depth_limit {
			f.push_str(&format!(" DEPTH {depth}"));
		}
		if let Some(complexity) = self.complexity_limit {
			f.push_str(&format!(" COMPLEXITY {complexity}"));
		}
		// Only emit INTROSPECTION clause when it differs from the default (AUTO)
		if matches!(self.introspection, IntrospectionConfig::None) {
			f.push_str(" INTROSPECTION ");
			self.introspection.fmt_sql(f, fmt);
		}
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
		EscapeKwFreeIdent(&self.name).fmt_sql(f, fmt);
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
