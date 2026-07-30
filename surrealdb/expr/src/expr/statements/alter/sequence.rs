use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AlterSequenceStatement {
	pub name: Expr,
	pub if_exists: bool,
	pub timeout: Option<Expr>,
}

impl Default for AlterSequenceStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			timeout: None,
		}
	}
}

impl ToSql for AlterSequenceStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterSequenceStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
