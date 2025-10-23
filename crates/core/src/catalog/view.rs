use revision::revisioned;
use surrealdb_types::{ToSql, write_sql};

use crate::dbs::aggregation::AggregationAnalysis;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Fields, Groups, Idiom};
use crate::sql::View;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Aggregation {
	Count,
	CountFn {
		arg: usize,
	},
	NumMax {
		arg: usize,
	},
	NumMin {
		arg: usize,
	},
	NumSum {
		arg: usize,
	},
	NumMean {
		arg: usize,
	},
	TimeMax {
		arg: usize,
	},
	TimeMin {
		arg: usize,
	},
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ViewDefinition {
	pub(crate) fields: Fields,
	pub(crate) what: Vec<String>,
	pub(crate) cond: Option<Expr>,
	pub(crate) groups: Option<Groups>,
	pub(crate) _tmp: ViewDefinition2,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ViewDefinition2 {
	/// The view is cached, and has no aggregation.
	/// It is only updated any of the target tables are updated.
	Materialized {
		fields: Fields,
		target_tables: Vec<String>,
		condition: Option<Expr>,
	},
	/// The view has a group by and has a running compute.
	Aggregated(AggregationAnalysis),
	/// The view is computed by doing another select query.
	Select {
		fields: Fields,
		tables: Vec<String>,
		cond: Option<Expr>,
		groups: Option<Groups>,
	},
}

impl ViewDefinition {
	pub(crate) fn to_sql_definition(&self) -> View {
		View {
			expr: self.fields.clone().into(),
			what: self.what.clone(),
			cond: self.cond.clone().map(|e| crate::sql::Cond(e.into())),
			group: self.groups.clone().map(Into::into),
		}
	}
}

impl ToSql for ViewDefinition {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "{}", self.to_sql_definition())
	}
}
impl InfoStructure for ViewDefinition {
	fn structure(self) -> Value {
		self.to_sql().into()
	}
}
