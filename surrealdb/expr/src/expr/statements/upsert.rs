use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::{Cond, Data, Explain, Expr, Literal, Output, With};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct UpsertStatement {
	pub only: bool,
	pub what: Vec<Expr>,
	pub with: Option<With>,
	pub data: Option<Data>,
	pub cond: Option<Cond>,
	pub output: Option<Output>,
	pub timeout: Expr,
	pub explain: Option<Explain>,
}

impl Default for UpsertStatement {
	fn default() -> Self {
		Self {
			only: Default::default(),
			what: Default::default(),
			with: Default::default(),
			data: Default::default(),
			cond: Default::default(),
			output: Default::default(),
			timeout: Expr::Literal(Literal::None),
			explain: Default::default(),
		}
	}
}

impl ToSql for UpsertStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::upsert::UpsertStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
