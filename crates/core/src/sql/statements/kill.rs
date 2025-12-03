use surrealdb_types::{SqlFormat, ToSql};

use crate::sql::Expr;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct KillStatement {
	// Uuid of Live Query
	// or Param resolving to Uuid of Live Query
	pub id: Expr,
}

impl ToSql for KillStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("KILL ");
		self.id.fmt_sql(f, fmt);
	}
}

impl From<KillStatement> for crate::expr::statements::KillStatement {
	fn from(v: KillStatement) -> Self {
		Self {
			id: v.id.into(),
		}
	}
}

impl From<crate::expr::statements::KillStatement> for KillStatement {
	fn from(v: crate::expr::statements::KillStatement) -> Self {
		Self {
			id: v.id.into(),
		}
	}
}
