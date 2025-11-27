
use surrealdb_types::{SqlFormat, ToSql, write_sql};
use uuid::Uuid;

use crate::fmt::CoverStmtsSql;
use crate::sql::{Cond, Expr, Fetchs, Fields};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveStatement {
	pub fields: Fields,
	pub diff: bool,
	pub what: Expr,
	pub cond: Option<Cond>,
	pub fetch: Option<Fetchs>,
}

impl ToSql for LiveStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "LIVE SELECT");
		if self.diff {
			write_sql!(f, fmt, " DIFF");
		}
		if !self.fields.is_empty() {
			write_sql!(f, fmt, " {}", self.fields);
		}
		write_sql!(f, fmt, " FROM {}", CoverStmtsSql(&self.what));
		if let Some(ref v) = self.cond {
			write_sql!(f, fmt, " {v}");
		}
		if let Some(ref v) = self.fetch {
			write_sql!(f, fmt, " {v}");
		}
	}
}

impl From<LiveStatement> for crate::expr::statements::LiveStatement {
	fn from(v: LiveStatement) -> Self {
		crate::expr::statements::LiveStatement {
			id: Uuid::new_v4(),
			node: Uuid::new_v4(),
			fields: v.fields.into(),
			diff: v.diff,
			what: v.what.into(),
			cond: v.cond.map(Into::into),
			fetch: v.fetch.map(Into::into),
		}
	}
}
impl From<crate::expr::statements::LiveStatement> for LiveStatement {
	fn from(v: crate::expr::statements::LiveStatement) -> Self {
		LiveStatement {
			fields: v.fields.into(),
			diff: v.diff,
			what: v.what.into(),
			cond: v.cond.map(Into::into),
			fetch: v.fetch.map(Into::into),
		}
	}
}
