use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::{Data, Expr, Output, Timeout};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct CreateStatement {
	// A keyword modifier indicating if we are expecting a single result or several
	pub only: bool,
	// Where we are creating (i.e. table, or record ID)
	pub what: Vec<Expr>,
	// The data associated with the record being created
	pub data: Option<Data>,
	//  What the result of the statement should resemble (i.e. Diff or no result etc).
	pub output: Option<Output>,
	// The timeout for the statement
	pub timeout: Option<Timeout>,
	// If the statement should be run in parallel
	pub parallel: bool,
	// Version as nanosecond timestamp passed down to Datastore
	pub version: Option<Expr>,
}

impl ToSql for CreateStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		f.push_str("CREATE");
		if self.only {
			f.push_str(" ONLY");
		}
		f.push(' ');
		for (i, expr) in self.what.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			expr.fmt_sql(f, sql_fmt);
		}
		if let Some(ref v) = self.data {
			f.push(' ');
			v.fmt_sql(f, sql_fmt);
		}
		if let Some(ref v) = self.output {
			write_sql!(f, sql_fmt, " {}", v);
		}
		if let Some(ref v) = self.version {
			write_sql!(f, sql_fmt, "VERSION {v}");
		}
		if let Some(ref v) = self.timeout {
			write_sql!(f, sql_fmt, " {v}");
		}
		if self.parallel {
			f.push_str(" PARALLEL");
		}
	}
}

impl From<CreateStatement> for crate::expr::statements::CreateStatement {
	fn from(v: CreateStatement) -> Self {
		crate::expr::statements::CreateStatement {
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			data: v.data.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			version: v.version.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::CreateStatement> for CreateStatement {
	fn from(v: crate::expr::statements::CreateStatement) -> Self {
		CreateStatement {
			only: v.only,
			what: v.what.into_iter().map(From::from).collect(),
			data: v.data.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			version: v.version.map(Into::into),
		}
	}
}
