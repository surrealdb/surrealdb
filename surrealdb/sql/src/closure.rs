use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::{CoverStmts, Expr, Kind, Param};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Closure {
	pub args: Vec<(Param, Kind)>,
	pub returns: Option<Kind>,
	pub body: Expr,
}

impl ToSql for Closure {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "|");
		for (i, (name, kind)) in self.args.iter().enumerate() {
			if i > 0 {
				write_sql!(f, fmt, ", ");
			}
			write_sql!(f, fmt, "{name}: ");
			match kind {
				k @ Kind::Either(_) => write_sql!(f, fmt, "<{}>", k),
				k => write_sql!(f, fmt, "{}", k),
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
