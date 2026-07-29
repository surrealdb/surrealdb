use common::fmt::Fmt;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::order::Ordering;
use crate::{
	Cond, CoverStmts, Explain, Expr, Fetchs, Fields, Groups, Limit, Literal, Splits, Start, With,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectStatement {
	/// The foo,bar part in SELECT foo,bar FROM baz.
	pub fields: Fields,
	pub omit: Vec<Expr>,
	pub only: bool,
	/// The baz part in SELECT foo,bar FROM baz.
	pub what: Vec<Expr>,
	pub with: Option<With>,
	pub cond: Option<Cond>,
	pub split: Option<Splits>,
	pub group: Option<Groups>,
	pub order: Option<Ordering>,
	pub limit: Option<Limit>,
	pub start: Option<Start>,
	pub fetch: Option<Fetchs>,
	pub version: Expr,
	pub timeout: Expr,
	pub explain: Option<Explain>,
	pub tempfiles: bool,
}

impl ToSql for SelectStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "SELECT {}", self.fields);
		if !self.omit.is_empty() {
			write_sql!(f, fmt, " OMIT {}", Fmt::comma_separated(self.omit.iter().map(CoverStmts)));
		}
		write_sql!(f, fmt, " FROM");
		if self.only {
			write_sql!(f, fmt, " ONLY");
		}
		write_sql!(f, fmt, " {}", Fmt::comma_separated(self.what.iter().map(CoverStmts)));
		if let Some(ref v) = self.with {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.cond {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.split {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.group {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.order {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.limit {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.start {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.fetch {
			write_sql!(f, fmt, " {v}");
		}
		if !matches!(self.version, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " VERSION {}", CoverStmts(&self.version));
		}
		if !matches!(self.timeout, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " TIMEOUT {}", CoverStmts(&self.timeout));
		}
		if let Some(ref v) = self.explain {
			write_sql!(f, fmt, " {v}");
		}
	}
}
