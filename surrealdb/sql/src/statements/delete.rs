use common::fmt::Fmt;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::{Cond, CoverStmts, Explain, Expr, Literal, Output, With};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DeleteStatement {
	pub only: bool,
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::arbitrary::atleast_one))]
	pub what: Vec<Expr>,
	pub with: Option<With>,
	pub cond: Option<Cond>,
	pub output: Option<Output>,
	pub timeout: Expr,
	pub explain: Option<Explain>,
}

impl Default for DeleteStatement {
	fn default() -> Self {
		Self {
			only: Default::default(),
			what: Default::default(),
			with: Default::default(),
			cond: Default::default(),
			output: Default::default(),
			timeout: Expr::Literal(Literal::None),
			explain: Default::default(),
		}
	}
}
impl ToSql for DeleteStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "DELETE");
		if self.only {
			f.push_str(" ONLY")
		}
		write_sql!(f, fmt, " {}", Fmt::comma_separated(self.what.iter().map(CoverStmts)));
		if let Some(ref v) = self.with {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.cond {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.output {
			write_sql!(f, fmt, " {v}");
		}

		if !matches!(self.timeout, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " TIMEOUT {}", CoverStmts(&self.timeout));
		}

		if let Some(ref v) = self.explain {
			write_sql!(f, fmt, " {v}");
		}
	}
}
