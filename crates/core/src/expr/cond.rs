use std::fmt;

use revision::revisioned;

use super::Expr;
use super::expression::VisitExpression;
use crate::sql::{ToSql, write_sql};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct Cond(pub(crate) Expr);

impl fmt::Display for Cond {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "WHERE {}", self.0)
	}
}

impl ToSql for Cond {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "WHERE {}", self.0);
	}
}

impl VisitExpression for Cond {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		self.0.visit(visitor);
	}
}
