use common::fmt::{EscapeKwFreeIdent, QuoteStr};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::expr::user::UserDuration;
use crate::expr::{Base, CoverStmts, Expr, Literal};
use crate::iam::ScramCredential;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineUserStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub base: Base,
	/// Plaintext awaiting preparation at the statement execution boundary.
	pub pending_password: Option<crate::expr::user::PendingPassword>,
	pub hash: String,
	/// Preserve an explicitly supplied empty PASSHASH in canonical text.
	pub hash_is_set: bool,
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
			pending_password: None,
			hash: String::new(),
			hash_is_set: false,
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
		self.fmt_user_sql(f, fmt, false);
	}
}

impl DefineUserStatement {
	/// Renders user metadata with password-equivalent credentials redacted.
	pub fn fmt_redacted_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.fmt_user_sql(f, fmt, true);
	}

	fn fmt_user_sql(&self, f: &mut String, fmt: SqlFormat, redact: bool) {
		write_sql!(f, fmt, "DEFINE USER");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, fmt, " IF NOT EXISTS"),
		}

		write_sql!(f, fmt, " {} ON {}", CoverStmts(&self.name), &self.base);

		if redact {
			let hash = if self.hash.is_empty() && self.pending_password.is_none() {
				""
			} else {
				"[REDACTED]"
			};
			write_sql!(f, fmt, " PASSHASH {}", QuoteStr(hash));
		} else if let Some(password) = &self.pending_password {
			write_sql!(f, fmt, " PASSWORD {}", QuoteStr(&password.0));
		} else if self.hash_is_set || !self.hash.is_empty() {
			write_sql!(f, fmt, " PASSHASH {}", QuoteStr(&self.hash));
		}

		if let Some(ref x) = self.scram {
			if redact {
				write_sql!(f, fmt, " PASSSCRAM {}", QuoteStr("[REDACTED]"));
			} else {
				write_sql!(f, fmt, " PASSSCRAM {}", QuoteStr(&x.to_verifier_string()));
			}
		}

		write_sql!(f, fmt, " ROLES ");
		for (idx, r) in self.roles.iter().enumerate() {
			if idx != 0 {
				f.push_str(", ");
			}

			let r = r.to_uppercase();
			EscapeKwFreeIdent(&r).fmt_sql(f, fmt);
		}

		// Always print relevant durations so defaults can be changed in the future
		// If default values were not printed, exports would not be forward compatible
		// None values need to be printed, as they are different from the default values
		f.push_str(" DURATION FOR TOKEN ");
		CoverStmts(&self.duration.token).fmt_sql(f, fmt);
		f.push_str(", FOR SESSION ");
		CoverStmts(&self.duration.session).fmt_sql(f, fmt);
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
	}
}
