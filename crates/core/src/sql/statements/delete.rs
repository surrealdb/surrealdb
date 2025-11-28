use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::{CoverStmtsSql, Fmt};
use crate::sql::{Cond, Explain, Expr, Output, Timeout, With};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DeleteStatement {
	pub only: bool,
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
	pub what: Vec<Expr>,
	pub with: Option<With>,
	pub cond: Option<Cond>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
	pub explain: Option<Explain>,
}

impl ToSql for DeleteStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "DELETE");
		if self.only {
			f.push_str(" ONLY")
		}
		write_sql!(f, fmt, " {}", Fmt::comma_separated(self.what.iter().map(CoverStmtsSql)));
		if let Some(ref v) = self.with {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.cond {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.output {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.timeout {
			write_sql!(f, fmt, " {v}");
		}
		if self.parallel {
			f.push_str(" PARALLEL");
		}
		if let Some(ref v) = self.explain {
			write_sql!(f, fmt, " {v}");
		}
	}
}

impl From<DeleteStatement> for crate::expr::statements::DeleteStatement {
	fn from(v: DeleteStatement) -> Self {
		crate::expr::statements::DeleteStatement {
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			with: v.with.map(Into::into),
			cond: v.cond.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			explain: v.explain.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::DeleteStatement> for DeleteStatement {
	fn from(v: crate::expr::statements::DeleteStatement) -> Self {
		DeleteStatement {
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			with: v.with.map(Into::into),
			cond: v.cond.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			explain: v.explain.map(Into::into),
		}
	}
}
