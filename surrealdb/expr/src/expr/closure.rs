use std::cmp::Ordering;

use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::{Expr, Kind, Param};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ClosureExpr {
	pub args: Vec<(Param, Kind)>,
	pub returns: Option<Kind>,
	pub body: Expr,
}

impl PartialOrd for ClosureExpr {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}
impl Ord for ClosureExpr {
	fn cmp(&self, _: &Self) -> Ordering {
		Ordering::Equal
	}
}

impl ToSql for ClosureExpr {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let closure: crate::sql::Closure = self.clone().into();
		closure.fmt_sql(f, sql_fmt);
	}
}
