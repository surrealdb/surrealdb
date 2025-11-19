use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::{Cond, Explain, Expr, Output, Timeout, With};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DeleteStatement {
	pub only: bool,
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
		f.push_str("DELETE");
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
