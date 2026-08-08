use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::Expr;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Start(pub Expr);

impl Start {
	/// Whether evaluating the start expression can be done on a read-only
	/// transaction.
	pub fn read_only(&self) -> bool {
		self.0.read_only()
	}
}

impl ToSql for Start {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let sql_start: crate::sql::Start = self.clone().into();
		sql_start.fmt_sql(f, fmt);
	}
}
