use std::fmt::Debug;

use surrealdb_strand::TableName;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::statements::info::InfoStructure;
use crate::expr::{Cond, Fields, Groups, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct View {
	pub expr: Fields,
	pub what: Vec<TableName>,
	pub cond: Option<Cond>,
	pub group: Option<Groups>,
}

impl ToSql for View {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let sql_view: crate::sql::View = self.clone().into();
		sql_view.fmt_sql(f, fmt);
	}
}
impl InfoStructure for View {
	fn structure(self) -> Value {
		self.to_sql().into()
	}
}
