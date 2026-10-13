use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::expr::{Expr, Literal};
use crate::sql::EventKind;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AlterEventStatement {
	pub name: Expr,
	pub what: Expr,
	pub if_exists: bool,
	pub when: AlterKind<Expr>,
	pub then: AlterKind<Vec<Expr>>,
	pub comment: AlterKind<String>,
	pub kind: AlterKind<EventKind>,
}

impl Default for AlterEventStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			if_exists: false,
			when: AlterKind::None,
			then: AlterKind::None,
			comment: AlterKind::None,
			kind: AlterKind::None,
		}
	}
}

impl ToSql for AlterEventStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterEventStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
