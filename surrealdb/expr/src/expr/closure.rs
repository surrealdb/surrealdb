use std::cmp::Ordering;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::{CoverStmts, Expr, Kind, Param};

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
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "|");
		for (i, (name, kind)) in self.args.iter().enumerate() {
			if i > 0 {
				write_sql!(f, fmt, ", ");
			}
			write_sql!(f, fmt, "{name}: ");
			if matches!(kind, Kind::Either(_)) {
				write_sql!(f, fmt, "<{}>", kind);
			} else {
				write_sql!(f, fmt, "{}", kind);
			}
		}
		write_sql!(f, fmt, "|");
		if let Some(returns) = &self.returns {
			write_sql!(f, fmt, " -> {returns}");
		}
		//  To avoid for example || ->? where ->? is a graph from failing to parse because the
		//  parser expects a kind after ->
		if self.body.has_left_idiom() {
			write_sql!(f, fmt, " ({})", &self.body)
		} else {
			write_sql!(f, fmt, " {}", CoverStmts(&self.body))
		}
	}
}
