use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::{Data, Expr, Output};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct InsertStatement {
	pub into: Option<Expr>,
	pub data: Data,
	/// Does the statement have the ignore clause.
	pub ignore: bool,
	pub update: Option<Data>,
	pub output: Option<Output>,
	pub timeout: Expr,
	pub relation: bool,
}

impl ToSql for InsertStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::insert::InsertStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
