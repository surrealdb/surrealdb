use std::fmt;

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

impl fmt::Display for UpdateStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "UPDATE")?;
		if self.only {
			f.write_str(" ONLY")?
		}
		write!(f, " {}", Fmt::comma_separated(self.what.iter()))?;
		if let Some(ref v) = self.with {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.data {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.cond {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.output {
			write!(f, " {v}")?
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

impl ToSql for UpdateStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("UPDATE");
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
		if let Some(ref v) = self.data {
			f.push(' ');
			v.fmt_sql(f, fmt);
		}
		if let Some(ref v) = self.cond {
			write_sql!(f, " {}", v);
		}
		if let Some(ref v) = self.output {
			write_sql!(f, " {}", v);
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
