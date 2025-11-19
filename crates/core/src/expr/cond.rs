use std::fmt;

use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use super::Expr;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct Cond(pub(crate) Expr);

impl ToSql for Cond {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("WHERE ");
		self.0.fmt_sql(f, fmt);
	}
}

impl fmt::Display for Cond {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// Convert to sql module type and use its Display implementation
		let sql_cond: crate::sql::Cond = self.clone().into();
		fmt::Display::fmt(&sql_cond, f)
	}
}
