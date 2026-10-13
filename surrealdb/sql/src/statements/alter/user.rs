use common::fmt::{EscapeKwFreeIdent, QuoteStr, SqlDuration};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::statements::define::user::PassType;
use crate::{Base, CoverStmts, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER USER`.
pub struct AlterUserStatement {
	pub name: Expr,
	pub base: Base,
	pub if_exists: bool,
	pub pass_type: Option<PassType>,
	/// Explicit SCRAM verifier string (`PASSSCRAM`). Mostly for symmetry with
	/// `DEFINE USER`; import always emits `DEFINE`.
	// A SCRAM verifier must be a valid `SCRAM-SHA-256$...` string, which
	// arbitrary bytes won't satisfy; leave it unset for fuzzing.
	#[cfg_attr(feature = "arbitrary", arbitrary(default))]
	pub scram: Option<String>,
	pub roles: AlterKind<Vec<String>>,
	pub token_duration: AlterKind<std::time::Duration>,
	pub session_duration: AlterKind<std::time::Duration>,
	pub comment: AlterKind<String>,
}

impl Default for AlterUserStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			base: Base::Root,
			if_exists: false,
			pass_type: None,
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

		if let Some(ref pt) = self.pass_type {
			match pt {
				PassType::Password(p) => write_sql!(f, fmt, " PASSWORD {}", QuoteStr(p)),
				PassType::Hash(h) => write_sql!(f, fmt, " PASSHASH {}", QuoteStr(h)),
				PassType::Unset => {}
			}
		}

		if let Some(ref x) = self.scram {
			write_sql!(f, fmt, " PASSSCRAM {}", QuoteStr(x));
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
			AlterKind::Set(d) => write_sql!(f, fmt, " DURATION FOR TOKEN {}", SqlDuration(d)),
			AlterKind::Drop => f.push_str(" DURATION FOR TOKEN NONE"),
			AlterKind::None => {}
		}

		match self.session_duration {
			AlterKind::Set(d) => write_sql!(f, fmt, " DURATION FOR SESSION {}", SqlDuration(d)),
			AlterKind::Drop => f.push_str(" DURATION FOR SESSION NONE"),
			AlterKind::None => {}
		}

		match self.comment {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}
	}
}
