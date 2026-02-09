use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::aggregation::AggregationAnalysis;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Fields, Groups};
use crate::sql::{Cond, View};
use crate::val::{TableName, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ViewDefinition {
	/// The view is cached, and has no aggregation.
	/// It is only updated any of the target tables are updated.
	Materialized {
		fields: Fields,
		tables: Vec<TableName>,
		condition: Option<Expr>,
	},
	/// The view has a group by and has a running compute.
	Aggregated {
		analysis: AggregationAnalysis,
		condition: Option<Expr>,
		tables: Vec<TableName>,
		// fields below are only used for reconstructing the query.
		groups: Groups,
		fields: Fields,
	},
	/// The view is computed by doing another select query.
	Select {
		fields: Fields,
		tables: Vec<TableName>,
		condition: Option<Expr>,
		groups: Option<Groups>,
	},
}

impl ViewDefinition {
	pub(crate) fn to_sql_definition(&self) -> View {
		match self {
			ViewDefinition::Materialized {
				fields,
				tables,
				condition,
			} => View {
				expr: fields.clone().into(),
				what: tables.clone().into_iter().map(|x| x.into_string()).collect(),
				cond: condition.clone().map(|x| Cond(x.into())),
				group: None,
			},
			ViewDefinition::Aggregated {
				tables,
				condition,
				groups,
				fields,
				..
			} => View {
				expr: fields.clone().into(),
				what: tables.clone().into_iter().map(|x| x.into_string()).collect(),
				cond: condition.clone().map(|x| Cond(x.into())),
				group: Some(groups.clone().into()),
			},
			ViewDefinition::Select {
				fields,
				tables,
				condition,
				groups,
			} => View {
				expr: fields.clone().into(),
				what: tables.clone().into_iter().map(|x| x.into_string()).collect(),
				cond: condition.clone().map(|x| Cond(x.into())),
				group: groups.clone().map(|x| x.into()),
			},
		}
	}
}

impl ToSql for ViewDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}
impl InfoStructure for ViewDefinition {
	fn structure(self) -> Value {
		self.to_sql().into()
	}
}
