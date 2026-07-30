use std::time::Duration;

use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::expr::{Base, Expr, Literal};
use crate::iam::ScramCredential;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AlterUserStatement {
	pub name: Expr,
	pub base: Base,
	pub if_exists: bool,
	pub hash: Option<String>,
	/// SCRAM verifier change. Outer `Option`: whether the SCRAM field is being
	/// changed at all; inner `Option`: the new value (`None` clears it — e.g.
	/// when the password is set via `PASSHASH`, invalidating any prior verifier).
	pub scram: Option<Option<ScramCredential>>,
	pub roles: AlterKind<Vec<String>>,
	pub token_duration: AlterKind<Option<Duration>>,
	pub session_duration: AlterKind<Option<Duration>>,
	pub comment: AlterKind<String>,
}

impl Default for AlterUserStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			base: Base::Root,
			if_exists: false,
			hash: None,
			scram: None,
			roles: AlterKind::None,
			token_duration: AlterKind::None,
			session_duration: AlterKind::None,
			comment: AlterKind::None,
		}
	}
}

impl ToSql for AlterUserStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterUserStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
