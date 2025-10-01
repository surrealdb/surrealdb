use std::fmt::Write;

use revision::revisioned;
use surrealdb_types::sql::ToSql;

use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Fields, Groups};
use crate::sql::View;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ViewDefinition {
	pub(crate) fields: Fields,
	pub(crate) what: Vec<String>,
	pub(crate) cond: Option<Expr>,
	pub(crate) groups: Option<Groups>,
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
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		write!(f, "{}", self.to_sql_definition())
	}
}
impl InfoStructure for ViewDefinition {
	fn structure(self) -> Value {
		self.to_sql().unwrap_or_else(|_| "<error>".to_string()).into()
	}
}
