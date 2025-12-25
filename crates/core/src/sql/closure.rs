use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::CoverStmts;
use crate::sql::{Expr, Kind, Param};

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
		match &self.body {
			Expr::Idiom(_) => write_sql!(f, fmt, " ({})", &self.body),
			x => write_sql!(f, fmt, " {}", CoverStmts(x)),
		}
	}
}

impl From<Closure> for crate::expr::ClosureExpr {
	fn from(v: Closure) -> Self {
		Self {
			args: v.args.into_iter().map(|(i, k)| (i.into(), k.into())).collect(),
			returns: v.returns.map(Into::into),
			body: v.body.into(),
		}
	}
}

impl From<crate::expr::ClosureExpr> for Closure {
	fn from(v: crate::expr::ClosureExpr) -> Self {
		Self {
			args: v.args.into_iter().map(|(i, k)| (i.into(), k.into())).collect(),
			returns: v.returns.map(Into::into),
			body: v.body.into(),
		}
	}
}
