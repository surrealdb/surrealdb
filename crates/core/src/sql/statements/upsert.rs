use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::Fmt;
use crate::sql::{Cond, Data, Explain, Expr, Output, Timeout, With};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct UpsertStatement {
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

impl ToSql for UpsertStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "UPSERT");
		if self.only {
			write_sql!(f, sql_fmt, " ONLY");
		}
		write_sql!(f, sql_fmt, " {}", Fmt::comma_separated(self.what.iter()));
		if let Some(ref v) = self.with {
			write_sql!(f, sql_fmt, " {v}");
		}
		if let Some(ref v) = self.data {
			write_sql!(f, sql_fmt, " {v}");
		}
		if let Some(ref v) = self.cond {
			write_sql!(f, sql_fmt, " {v}");
		}
		if let Some(ref v) = self.output {
			write_sql!(f, sql_fmt, " {v}");
		}
		if let Some(ref v) = self.timeout {
			write_sql!(f, sql_fmt, " {v}");
		}
		if self.parallel {
			write_sql!(f, sql_fmt, " PARALLEL");
		}
		if let Some(ref v) = self.explain {
			write_sql!(f, sql_fmt, " {v}");
		}
	}
}

impl From<UpsertStatement> for crate::expr::statements::UpsertStatement {
	fn from(v: UpsertStatement) -> Self {
		Self {
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

impl From<crate::expr::statements::UpsertStatement> for UpsertStatement {
	fn from(v: crate::expr::statements::UpsertStatement) -> Self {
		Self {
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
