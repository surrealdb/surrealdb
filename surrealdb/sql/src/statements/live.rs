use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::{Cond, CoverStmts, Expr, Fetchs, Fields};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum LiveFields {
	Diff,
	Select(Fields),
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct LiveStatement {
	pub fields: LiveFields,
	pub what: Expr,
	pub cond: Option<Cond>,
	pub fetch: Option<Fetchs>,
}

impl ToSql for LiveStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("LIVE SELECT");
		match &self.fields {
			LiveFields::Diff => write_sql!(f, fmt, " DIFF"),
			LiveFields::Select(x) => write_sql!(f, fmt, " {}", x),
		}
		write_sql!(f, fmt, " FROM {}", CoverStmts(&self.what));
		if let Some(ref v) = self.cond {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.fetch {
			write_sql!(f, fmt, " {v}");
		}
	}
}
