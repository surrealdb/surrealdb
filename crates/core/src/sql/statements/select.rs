use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::order::Ordering;
use crate::sql::{
	Cond, Explain, Expr, Fetchs, Fields, Groups, Limit, Splits, Start, Timeout, With,
};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct SelectStatement {
	/// The foo,bar part in SELECT foo,bar FROM baz.
	pub expr: Fields,
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
	pub version: Option<Expr>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
	pub explain: Option<Explain>,
	pub tempfiles: bool,
}

impl ToSql for SelectStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, "SELECT {}", self.expr);
		if !self.omit.is_empty() {
			f.push_str(" OMIT ");
			for (i, expr) in self.omit.iter().enumerate() {
				if i > 0 {
					f.push_str(", ");
				}
				expr.fmt_sql(f, fmt);
			}
		}
		f.push_str(" FROM");
		if self.only {
			f.push_str(" ONLY");
		}
		f.push(' ');
		for (i, expr) in self.what.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			expr.fmt_sql(f, fmt);
		}
		if let Some(ref v) = self.with {
			write_sql!(f, " {}", v);
		}
		if let Some(ref v) = self.cond {
			write_sql!(f, " {}", v);
		}
		if let Some(ref v) = self.split {
			write_sql!(f, " {}", v);
		}
		if let Some(ref v) = self.group {
			write_sql!(f, " {}", v);
		}
		if let Some(ref v) = self.order {
			write_sql!(f, " {}", v);
		}
		if let Some(ref v) = self.limit {
			write_sql!(f, " {}", v);
		}
		if let Some(ref v) = self.start {
			write_sql!(f, " {}", v);
		}
		if let Some(ref v) = self.fetch {
			write_sql!(f, " {}", v);
		}
		if let Some(ref v) = self.version {
			f.push_str(" VERSION ");
			v.fmt_sql(f, fmt);
		}
		if let Some(ref v) = self.timeout {
			write_sql!(f, " {}", v);
		}
		if self.parallel {
			f.push_str(" PARALLEL");
		}
		if let Some(ref v) = self.explain {
			write_sql!(f, " {}", v);
		}
	}
}

impl From<SelectStatement> for crate::expr::statements::SelectStatement {
	fn from(v: SelectStatement) -> Self {
		Self {
			expr: v.expr.into(),
			omit: v.omit.into_iter().map(Into::into).collect(),
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
			omit: v.omit.into_iter().map(Into::into).collect(),
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
