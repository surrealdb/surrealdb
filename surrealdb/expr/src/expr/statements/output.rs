use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::fetch::Fetchs;
use crate::expr::{CoverStmts, Expr};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct OutputStatement {
	pub what: Expr,
	pub fetch: Option<Fetchs>,
}

impl OutputStatement {
	/// Check if we require a writeable transaction
	pub fn read_only(&self) -> bool {
		self.what.read_only() && self.fetch.as_ref().map(|x| x.read_only()).unwrap_or(true)
	}
}

impl ToSql for OutputStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "RETURN {}", CoverStmts(&self.what));
		if let Some(ref v) = self.fetch {
			write_sql!(f, sql_fmt, " {}", v);
		}
	}
}
