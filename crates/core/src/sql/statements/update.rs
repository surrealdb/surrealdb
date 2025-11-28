use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::{CoverStmtsSql, Fmt};
use crate::sql::{Cond, Data, Explain, Expr, Output, Timeout, With};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct UpdateStatement {
	pub only: bool,
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
	pub what: Vec<Expr>,
	pub with: Option<With>,
	pub data: Option<Data>,
	pub cond: Option<Cond>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
	pub explain: Option<Explain>,
}

impl ToSql for UpdateStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "UPDATE");
		if self.only {
			write_sql!(f, fmt, " ONLY");
		}
		write_sql!(f, fmt, " {}", Fmt::comma_separated(self.what.iter().map(CoverStmtsSql)));
		if let Some(ref v) = self.with {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.data {
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
			write_sql!(f, fmt, " PARALLEL");
		}
		if let Some(ref v) = self.explain {
			write_sql!(f, fmt, " {v}");
		}
	}
}

impl From<UpdateStatement> for crate::expr::statements::UpdateStatement {
	fn from(v: UpdateStatement) -> Self {
		crate::expr::statements::UpdateStatement {
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			with: v.with.map(Into::into),
			data: v.data.map(Into::into),
			cond: v.cond.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			explain: v.explain.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::UpdateStatement> for UpdateStatement {
	fn from(v: crate::expr::statements::UpdateStatement) -> Self {
		UpdateStatement {
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			with: v.with.map(Into::into),
			data: v.data.map(Into::into),
			cond: v.cond.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			explain: v.explain.map(Into::into),
		}
	}
}
