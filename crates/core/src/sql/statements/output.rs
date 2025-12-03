use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::CoverStmts;
use crate::sql::Expr;
use crate::sql::fetch::Fetchs;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct OutputStatement {
	pub what: Expr,
	pub fetch: Option<Fetchs>,
}

impl ToSql for OutputStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "RETURN {}", CoverStmts(&self.what));
		if let Some(ref v) = self.fetch {
			write_sql!(f, sql_fmt, " {}", v);
		}
	}
}

impl From<OutputStatement> for crate::expr::statements::OutputStatement {
	fn from(v: OutputStatement) -> Self {
		crate::expr::statements::OutputStatement {
			what: v.what.into(),
			fetch: v.fetch.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::OutputStatement> for OutputStatement {
	fn from(v: crate::expr::statements::OutputStatement) -> Self {
		OutputStatement {
			what: v.what.into(),
			fetch: v.fetch.map(Into::into),
		}
	}
}
