use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoveEventStatement {
	pub name: Expr,
	pub table_name: Expr,
	pub if_exists: bool,
}

impl Default for RemoveEventStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			table_name: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}

impl ToSql for RemoveEventStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::remove::RemoveEventStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
