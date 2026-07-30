use std::time::Duration;

use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::expr::{Base, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AlterAccessStatement {
	pub name: Expr,
	pub base: Base,
	pub if_exists: bool,
	pub authenticate: AlterKind<Expr>,
	pub grant_duration: AlterKind<Option<Duration>>,
	pub token_duration: AlterKind<Option<Duration>>,
	pub session_duration: AlterKind<Option<Duration>>,
	pub comment: AlterKind<String>,
}

impl Default for AlterAccessStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			base: Base::Root,
			if_exists: false,
			authenticate: AlterKind::None,
			grant_duration: AlterKind::None,
			token_duration: AlterKind::None,
			session_duration: AlterKind::None,
			comment: AlterKind::None,
		}
	}
}

impl ToSql for AlterAccessStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterAccessStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
