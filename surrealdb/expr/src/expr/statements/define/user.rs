use surrealdb_types::{SqlFormat, ToSql};

use super::DefineKind;
use crate::expr::user::UserDuration;
use crate::expr::{Base, Expr, Literal};
use crate::iam::ScramCredential;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineUserStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub base: Base,
	pub hash: String,
	pub code: String,
	pub roles: Vec<String>,
	pub duration: UserDuration,
	pub comment: Expr,
	/// SCRAM-SHA-256 verifier material, derived alongside `hash` from a plaintext
	/// password (or supplied via `PASSSCRAM`). `None` for PASSHASH-only users.
	pub scram: Option<ScramCredential>,
}

impl Default for DefineUserStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			base: Base::Root,
			hash: String::new(),
			code: String::new(),
			roles: vec![],
			duration: UserDuration::default(),
			comment: Expr::Literal(Literal::None),
			scram: None,
		}
	}
}

impl ToSql for DefineUserStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::define::DefineUserStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
