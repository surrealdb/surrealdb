//! GraphQL exposure configuration.
//!
//! `GraphQLConfig` selects which tables and functions a database exposes over
//! GraphQL. It is shared vocabulary between the DEFINE CONFIG statement and
//! the catalog's config definition; both planes persist it, so the revisioned
//! shapes here are storage-stable.

use common::fmt::EscapeKwFreeIdent;
use revision::revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::statements::info::InfoStructure;
use crate::val::{TableName, Value};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct GraphQLConfig {
	pub tables: GraphQLTablesConfig,
	pub functions: GraphQLFunctionsConfig,
	/// Maximum query nesting depth. `None` means no limit.
	#[revision(start = 2)]
	pub depth_limit: Option<u32>,
	/// Maximum query complexity (total number of fields). `None` means no limit.
	#[revision(start = 2)]
	pub complexity_limit: Option<u32>,
	/// Controls whether GraphQL schema introspection is enabled.
	/// Defaults to `Auto` (introspection enabled).
	#[revision(start = 3)]
	pub introspection: GraphQLIntrospectionConfig,
}

impl InfoStructure for GraphQLConfig {
	fn structure(self) -> Value {
		Value::from(map!(
			"tables" => self.tables.structure(),
			"functions" => self.functions.structure(),
			"depth_limit", if let Some(d) = self.depth_limit => Value::from(d as i64),
			"complexity_limit", if let Some(c) = self.complexity_limit => Value::from(c as i64),
			"introspection", if let GraphQLIntrospectionConfig::None = self.introspection => Value::None,
		))
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum GraphQLTablesConfig {
	#[default]
	None,
	Auto,
	Include(Vec<TableName>),
	Exclude(Vec<TableName>),
}

impl InfoStructure for GraphQLTablesConfig {
	fn structure(self) -> Value {
		match self {
			GraphQLTablesConfig::None => Value::None,
			GraphQLTablesConfig::Auto => Value::String(Strand::new_static("AUTO")),
			GraphQLTablesConfig::Include(ts) => Value::from(map!(
				"include" => Value::Array(ts.into_iter().map(Value::Table).collect()),
			)),
			GraphQLTablesConfig::Exclude(ts) => Value::from(map!(
				"exclude" => Value::Array(ts.into_iter().map(Value::Table).collect()),
			)),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum GraphQLFunctionsConfig {
	#[default]
	None,
	Auto,
	Include(Vec<Strand>),
	Exclude(Vec<Strand>),
}

impl InfoStructure for GraphQLFunctionsConfig {
	fn structure(self) -> Value {
		match self {
			GraphQLFunctionsConfig::None => Value::None,
			GraphQLFunctionsConfig::Auto => Value::String(Strand::new_static("AUTO")),
			GraphQLFunctionsConfig::Include(fs) => Value::from(map!(
				"include" => Value::Array(fs.into_iter().map(Value::from).collect()),
			)),
			GraphQLFunctionsConfig::Exclude(fs) => Value::from(map!(
				"exclude" => Value::Array(fs.into_iter().map(Value::from).collect()),
			)),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum GraphQLIntrospectionConfig {
	#[default]
	Auto,
	None,
}

impl InfoStructure for GraphQLIntrospectionConfig {
	fn structure(self) -> Value {
		match self {
			GraphQLIntrospectionConfig::Auto => Value::String(Strand::new_static("AUTO")),
			GraphQLIntrospectionConfig::None => Value::None,
		}
	}
}

impl ToSql for GraphQLIntrospectionConfig {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self {
			GraphQLIntrospectionConfig::Auto => f.push_str("AUTO"),
			GraphQLIntrospectionConfig::None => f.push_str("NONE"),
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
		if matches!(self.introspection, GraphQLIntrospectionConfig::None) {
			f.push_str(" INTROSPECTION ");
			self.introspection.fmt_sql(f, fmt);
		}
	}
}
impl ToSql for GraphQLTablesConfig {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			GraphQLTablesConfig::Auto => f.push_str("AUTO"),
			GraphQLTablesConfig::None => f.push_str("NONE"),
			GraphQLTablesConfig::Include(cs) => {
				f.push_str("INCLUDE ");
				for (i, table) in cs.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					common::fmt::EscapeIdent(table.as_str()).fmt_sql(f, fmt);
				}
			}
			GraphQLTablesConfig::Exclude(cs) => {
				f.push_str("EXCLUDE ");
				for (i, table) in cs.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					common::fmt::EscapeIdent(table.as_str()).fmt_sql(f, fmt);
				}
			}
		}
	}
}
impl ToSql for GraphQLFunctionsConfig {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			GraphQLFunctionsConfig::Auto => f.push_str("AUTO"),
			GraphQLFunctionsConfig::None => f.push_str("NONE"),
			GraphQLFunctionsConfig::Include(cs) => {
				f.push_str("INCLUDE ");
				fmt_function_name_list(f, fmt, cs);
			}
			GraphQLFunctionsConfig::Exclude(cs) => {
				f.push_str("EXCLUDE ");
				fmt_function_name_list(f, fmt, cs);
			}
		}
	}
}
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
