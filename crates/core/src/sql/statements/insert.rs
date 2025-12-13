use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::CoverStmts;
use crate::sql::{Data, Expr, Literal, Output};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InsertStatement {
	pub into: Expr,
	pub data: Data,
	/// Does the statement have the ignore clause.
	pub ignore: bool,
	pub update: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Expr,
	pub parallel: bool,
	pub relation: bool,
	pub version: Expr,
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
		write_sql!(f, fmt, " INTO {}", CoverStmts(&self.into));
		write_sql!(f, fmt, " {}", self.data);
		if let Some(ref v) = self.update {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.output {
			write_sql!(f, fmt, " {v}");
		}
		if !matches!(self.version, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " VERSION {}", CoverStmts(&self.version));
		}
		if !matches!(self.timeout, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " TIMEOUT {}", CoverStmts(&self.timeout));
		}
		if self.parallel {
			write_sql!(f, fmt, " PARALLEL");
		}
	}
}

impl From<InsertStatement> for crate::expr::statements::InsertStatement {
	fn from(v: InsertStatement) -> Self {
		crate::expr::statements::InsertStatement {
			into: v.into.into(),
			data: v.data.into(),
			ignore: v.ignore,
			update: v.update.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.into(),
			parallel: v.parallel,
			relation: v.relation,
			version: v.version.into(),
		}
	}
}

impl From<crate::expr::statements::InsertStatement> for InsertStatement {
	fn from(v: crate::expr::statements::InsertStatement) -> Self {
		InsertStatement {
			into: v.into.into(),
			data: v.data.into(),
			ignore: v.ignore,
			update: v.update.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.into(),
			parallel: v.parallel,
			relation: v.relation,
			version: v.version.into(),
		}
	}
}
