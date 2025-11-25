use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::{Data, Expr, Output, Timeout};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RelateStatement {
	pub only: bool,
	/// The expression through which we create a relation
	pub through: Expr,
	/// The expression the relation is from
	pub from: Expr,
	/// The expression the relation targets.
	pub to: Expr,
	pub uniq: bool,
	pub data: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
}

impl ToSql for RelateStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		f.push_str("RELATE");
		if self.only {
			f.push_str(" ONLY");
		}
		write_sql!(f, sql_fmt, " {} -> {} -> {}", self.from, self.through, self.to);
		if self.uniq {
			f.push_str(" UNIQUE");
		}
		if let Some(ref v) = self.data {
			write_sql!(f, sql_fmt, " {}", v);
		}
		if let Some(ref v) = self.output {
			write_sql!(f, sql_fmt, " {v}");
		}
		if let Some(ref v) = self.timeout {
			write_sql!(f, sql_fmt, " {v}");
		}
		if self.parallel {
			f.push_str(" PARALLEL");
		}
	}
}

impl From<RelateStatement> for crate::expr::statements::RelateStatement {
	fn from(v: RelateStatement) -> Self {
		crate::expr::statements::RelateStatement {
			only: v.only,
			through: v.through.into(),
			from: v.from.into(),
			to: v.to.into(),
			uniq: v.uniq,
			data: v.data.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
		}
	}
}

impl From<crate::expr::statements::RelateStatement> for RelateStatement {
	fn from(v: crate::expr::statements::RelateStatement) -> Self {
		RelateStatement {
			only: v.only,
			through: v.through.into(),
			from: v.from.into(),
			to: v.to.into(),
			uniq: v.uniq,
			data: v.data.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
		}
	}
}
