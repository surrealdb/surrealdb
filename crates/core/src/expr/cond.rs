use std::fmt::{self, Write};

use revision::revisioned;
use surrealdb_types::sql::ToSql;

use super::Expr;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct Cond(pub(crate) Expr);

impl fmt::Display for Cond {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "WHERE {}", self.0)
	}
}

impl ToSql for Cond {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		write!(f, "WHERE {}", self.0)
	}
}
