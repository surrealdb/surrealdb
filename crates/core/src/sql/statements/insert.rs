use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::{Data, Expr, Output, Timeout};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct InsertStatement {
	pub into: Option<Expr>,
	pub data: Data,
	/// Does the statement have the ignore clause.
	pub ignore: bool,
	pub update: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Option<Timeout>,
	pub parallel: bool,
	pub relation: bool,
	pub version: Option<Expr>,
}

impl ToSql for InsertStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		f.push_str("INSERT");
		if self.relation {
			f.push_str(" RELATION");
		}
		if self.ignore {
			f.push_str(" IGNORE");
		}
		if let Some(into) = &self.into {
			f.push_str(" INTO ");
			into.fmt_sql(f, sql_fmt);
		}
		f.push(' ');
		self.data.fmt_sql(f, sql_fmt);
		if let Some(ref v) = self.update {
			f.push(' ');
			v.fmt_sql(f, sql_fmt);
		}
		if let Some(ref v) = self.output {
			write_sql!(f, sql_fmt, " {}", v);
		}
		if let Some(ref v) = self.version {
			f.push_str(" VERSION ");
			v.fmt_sql(f, sql_fmt);
		}
		if let Some(ref v) = self.timeout {
			write_sql!(f, sql_fmt, " {}", v);
		}
		if self.parallel {
			f.push_str(" PARALLEL");
		}
	}
}

impl From<InsertStatement> for crate::expr::statements::InsertStatement {
	fn from(v: InsertStatement) -> Self {
		crate::expr::statements::InsertStatement {
			into: v.into.map(Into::into),
			data: v.data.into(),
			ignore: v.ignore,
			update: v.update.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			relation: v.relation,
			version: v.version.map(From::from),
		}
	}
}

impl From<crate::expr::statements::InsertStatement> for InsertStatement {
	fn from(v: crate::expr::statements::InsertStatement) -> Self {
		InsertStatement {
			into: v.into.map(Into::into),
			data: v.data.into(),
			ignore: v.ignore,
			update: v.update.map(Into::into),
			output: v.output.map(Into::into),
			timeout: v.timeout.map(Into::into),
			parallel: v.parallel,
			relation: v.relation,
			version: v.version.map(From::from),
		}
	}
}
