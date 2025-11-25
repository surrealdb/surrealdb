use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::Fmt;
use crate::sql::{Cond, Data, Explain, Expr, Output, Timeout, With};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct UpdateStatement {
	pub only: bool,
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
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "UPDATE");
		if self.only {
			write_sql!(f, sql_fmt, " ONLY");
		}
		write_sql!(f, sql_fmt, " {}", Fmt::comma_separated(self.what.iter()));
		if let Some(ref v) = self.with {
			write_sql!(f, sql_fmt, " {v}");
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
