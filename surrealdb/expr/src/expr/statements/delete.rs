use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::{Cond, Explain, Expr, Literal, Output, With};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DeleteStatement {
	pub only: bool,
	pub what: Vec<Expr>,
	pub with: Option<With>,
	pub cond: Option<Cond>,
	pub output: Option<Output>,
	pub timeout: Expr,
	pub explain: Option<Explain>,
}

impl Default for DeleteStatement {
	fn default() -> Self {
		Self {
			only: Default::default(),
			what: Default::default(),
			with: Default::default(),
			cond: Default::default(),
			output: Default::default(),
			timeout: Expr::Literal(Literal::None),
			explain: Default::default(),
		}
	}
}

impl ToSql for DeleteStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::delete::DeleteStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
