use revision::revisioned;

use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Fields, Groups};
use crate::sql::{ToSql, View};
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ViewDefinition {
	pub fields: Fields,
	pub what: Vec<String>,
	pub cond: Option<Expr>,
	pub groups: Option<Groups>,
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
	fn to_sql(&self) -> String {
		self.to_sql_definition().to_string()
	}
}
impl InfoStructure for ViewDefinition {
	fn structure(self) -> Value {
		self.to_sql().into()
	}
}
