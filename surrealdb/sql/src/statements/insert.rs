use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::{CoverStmts, Data, Expr, Literal, Output};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InsertStatement {
	pub into: Option<Expr>,
	pub data: Data,
	/// Does the statement have the ignore clause.
	pub ignore: bool,
	pub update: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Expr,
	pub relation: bool,
}

impl ToSql for InsertStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("INSERT");
		if self.relation {
			f.push_str(" RELATION");
		}
		if self.ignore {
			f.push_str(" IGNORE");
		}
		if let Some(ref v) = self.into {
			write_sql!(f, fmt, " INTO {}", CoverStmts(v));
		}
		write_sql!(f, fmt, " {}", self.data);
		if let Some(ref v) = self.update {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.output {
			write_sql!(f, fmt, " {v}");
		}
		if !matches!(self.timeout, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " TIMEOUT {}", CoverStmts(&self.timeout));
		}
	}
}
