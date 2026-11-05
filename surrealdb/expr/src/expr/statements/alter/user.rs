use std::time::Duration;

use common::fmt::{EscapeKwFreeIdent, QuoteStr, SqlDuration};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::expr::{Base, CoverStmts, Expr, Literal};
use crate::iam::ScramCredential;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AlterUserStatement {
	pub name: Expr,
	pub base: Base,
	pub if_exists: bool,
	/// Plaintext awaiting preparation at the statement execution boundary.
	pub pending_password: Option<crate::expr::user::PendingPassword>,
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
			pending_password: None,
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
		write_sql!(f, fmt, "ALTER USER");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {} ON {}", CoverStmts(&self.name), &self.base);

		if let Some(password) = &self.pending_password {
			write_sql!(f, fmt, " PASSWORD {}", QuoteStr(&password.0));
		} else if let Some(hash) = &self.hash {
			write_sql!(f, fmt, " PASSHASH {}", QuoteStr(hash));
		}

		if let Some(Some(ref x)) = self.scram {
			write_sql!(f, fmt, " PASSSCRAM {}", QuoteStr(&x.to_verifier_string()));
		}

		if let AlterKind::Set(ref roles) = self.roles {
			write_sql!(f, fmt, " ROLES");
			for (i, r) in roles.iter().enumerate() {
				if i > 0 {
					f.push(',');
				}
				write_sql!(f, fmt, " {}", EscapeKwFreeIdent(r));
			}
		}

		match self.token_duration {
			AlterKind::Set(Some(d)) => write_sql!(f, fmt, " DURATION FOR TOKEN {}", SqlDuration(d)),
			AlterKind::Set(None) | AlterKind::Drop => f.push_str(" DURATION FOR TOKEN NONE"),
			AlterKind::None => {}
		}

		match self.session_duration {
			AlterKind::Set(Some(d)) => {
				write_sql!(f, fmt, " DURATION FOR SESSION {}", SqlDuration(d))
			}
			AlterKind::Set(None) | AlterKind::Drop => f.push_str(" DURATION FOR SESSION NONE"),
			AlterKind::None => {}
		}

		match self.comment {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}
	}
}
