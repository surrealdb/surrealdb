use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::{CoverStmts, Fmt};
use crate::sql::{Cond, Data, Explain, Expr, Literal, Output, With};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct UpsertStatement {
	pub only: bool,
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
	pub what: Vec<Expr>,
	pub with: Option<With>,
	pub data: Option<Data>,
	pub cond: Option<Cond>,
	pub output: Option<Output>,
	pub timeout: Expr,
	pub explain: Option<Explain>,
}

impl Default for UpsertStatement {
	fn default() -> Self {
		Self {
			only: Default::default(),
			what: Default::default(),
			with: Default::default(),
			data: Default::default(),
			cond: Default::default(),
			output: Default::default(),
			timeout: Expr::Literal(Literal::None),
			explain: Default::default(),
		}
	}
}

impl ToSql for UpsertStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("UPSERT");
		if self.only {
			f.push_str(" ONLY")
		}
		write_sql!(f, fmt, " {}", Fmt::comma_separated(self.what.iter().map(CoverStmts)));
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
		if !matches!(self.timeout, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " TIMEOUT {}", CoverStmts(&self.timeout));
		}
		if let Some(ref v) = self.explain {
			write_sql!(f, fmt, " {v}");
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
			timeout: v.timeout.into(),
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
			timeout: v.timeout.into(),
			explain: v.explain.map(Into::into),
		}
	}
}
