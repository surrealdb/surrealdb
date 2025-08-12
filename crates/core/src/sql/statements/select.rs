use std::fmt;

use crate::sql::fmt::Fmt;
use crate::sql::order::Ordering;
use crate::sql::{
	Cond, Explain, Expr, Fetchs, Fields, Groups, Idioms, Limit, Splits, Start, Timeout, With,
};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct SelectStatement {
	/// The foo,bar part in SELECT foo,bar FROM baz.
	pub expr: Fields,
	pub omit: Option<Idioms>,
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
	pub version: Option<Expr>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
	pub explain: Option<Explain>,
	pub tempfiles: bool,
}

impl fmt::Display for SelectStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "SELECT {}", self.expr)?;
		if let Some(ref v) = self.omit {
			write!(f, " OMIT {v}")?
		}
		write!(f, " FROM")?;
		if self.only {
			f.write_str(" ONLY")?
		}
		write!(f, " {}", Fmt::comma_separated(self.what.iter()))?;
		if let Some(ref v) = self.with {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.cond {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.split {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.group {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.order {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.limit {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.start {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.fetch {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.version {
			write!(f, " VERSION {v}")?
		}
		if let Some(ref v) = self.timeout {
			write!(f, " {v}")?
		}
		if self.parallel {
			f.write_str(" PARALLEL")?
		}
		if let Some(ref v) = self.explain {
			write!(f, " {v}")?
		}
		Ok(())
	}
}

impl From<SelectStatement> for crate::expr::statements::SelectStatement {
	fn from(v: SelectStatement) -> Self {
		Self {
			expr: v.expr.into(),
			omit: v.omit.map(Into::into),
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			with: v.with.map(Into::into),
			cond: v.cond.map(Into::into),
			split: v.split.map(Into::into),
			group: v.group.map(Into::into),
			order: v.order.map(Into::into),
			limit: v.limit.map(Into::into),
			start: v.start.map(Into::into),
			fetch: v.fetch.map(Into::into),
			version: v.version.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			explain: v.explain.map(Into::into),
			tempfiles: v.tempfiles,
		}
	}
}

impl From<crate::expr::statements::SelectStatement> for SelectStatement {
	fn from(v: crate::expr::statements::SelectStatement) -> Self {
		Self {
			expr: v.expr.into(),
			omit: v.omit.map(Into::into),
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			with: v.with.map(Into::into),
			cond: v.cond.map(Into::into),
			split: v.split.map(Into::into),
			group: v.group.map(Into::into),
			order: v.order.map(Into::into),
			limit: v.limit.map(Into::into),
			start: v.start.map(Into::into),
			fetch: v.fetch.map(Into::into),
			version: v.version.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			explain: v.explain.map(Into::into),
			tempfiles: v.tempfiles,
		}
	}
}
