use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::fmt::EscapeKwFreeIdent;
use crate::val::TableName;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct GraphQLConfig {
	pub tables: TablesConfig,
	pub functions: FunctionsConfig,
	pub depth_limit: Option<u32>,
	pub complexity_limit: Option<u32>,
	pub introspection: IntrospectionConfig,
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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct TableConfig {
	pub name: TableName,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum FunctionsConfig {
	#[default]
	None,
	Auto,
	// Arbitrary generation is skipped: function names stored here are bare
	// `name(::name)*` identifiers (no `fn::` prefix), but `Strand`'s `Arbitrary`
	// impl yields any UTF-8 string, which the `INCLUDE`/`EXCLUDE` syntax
	// can't round-trip.
	#[cfg_attr(feature = "arbitrary", arbitrary(skip))]
	Include(Vec<Strand>),
	#[cfg_attr(feature = "arbitrary", arbitrary(skip))]
	Exclude(Vec<Strand>),
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
		EscapeKwFreeIdent(self.name.as_str()).fmt_sql(f, fmt);
	}
}

impl ToSql for FunctionsConfig {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			FunctionsConfig::Auto => f.push_str("AUTO"),
			FunctionsConfig::None => f.push_str("NONE"),
			FunctionsConfig::Include(cs) => {
				f.push_str("INCLUDE ");
				fmt_function_name_list(f, fmt, cs);
			}
			FunctionsConfig::Exclude(cs) => {
				f.push_str("EXCLUDE ");
				fmt_function_name_list(f, fmt, cs);
			}
		}
	}
}

/// Render a list of custom function names as a comma-separated sequence of
/// `fn::<name>` references — the same form the parser accepts, so the output
/// round-trips through `DEFINE CONFIG GRAPHQL FUNCTIONS INCLUDE/EXCLUDE`. Each
/// `::`-separated segment is escaped independently to keep the syntax legal
/// even when a segment is a SurrealQL keyword.
fn fmt_function_name_list(f: &mut String, fmt: SqlFormat, names: &[Strand]) {
	for (i, name) in names.iter().enumerate() {
		if i > 0 {
			f.push_str(", ");
		}
		f.push_str("fn::");
		let mut first = true;
		for segment in name.as_str().split("::") {
			if !first {
				f.push_str("::");
			}
			first = false;
			EscapeKwFreeIdent(segment).fmt_sql(f, fmt);
		}
	}
}
