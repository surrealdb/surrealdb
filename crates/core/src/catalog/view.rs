use revision::revisioned;
use surrealdb_types::{ToSql, write_sql};

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
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "{}", self.to_sql_definition())
	}
}
impl InfoStructure for ViewDefinition {
	fn structure(self) -> Value {
		self.to_sql().into()
	}
}
