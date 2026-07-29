use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fetch::Fetchs;
use crate::{CoverStmts, Expr};

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
