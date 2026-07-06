use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::catalog::ScramCredential;
use crate::fmt::{CoverStmts, EscapeKwFreeIdent, QuoteStr};
use crate::sql::statements::define::user::PassType;
use crate::sql::{Base, Expr, Literal};
use crate::types::PublicDuration;

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
	pub token_duration: AlterKind<PublicDuration>,
	pub session_duration: AlterKind<PublicDuration>,
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
			AlterKind::Set(ref d) => write_sql!(f, fmt, " DURATION FOR TOKEN {d}"),
			AlterKind::Drop => f.push_str(" DURATION FOR TOKEN NONE"),
			AlterKind::None => {}
		}

		match self.session_duration {
			AlterKind::Set(ref d) => write_sql!(f, fmt, " DURATION FOR SESSION {d}"),
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

impl From<AlterUserStatement> for crate::expr::statements::alter::AlterUserStatement {
	fn from(v: AlterUserStatement) -> Self {
		// Determine the SCRAM change before consuming `pass_type` for the hash.
		// An explicit PASSSCRAM wins; PASSWORD derives a new verifier; PASSHASH
		// clears any stale verifier (the plaintext is unknown); no clause leaves
		// SCRAM untouched. The verifier is validated at parse time, so `expect`
		// upholds that invariant loudly rather than silently clearing a bad one.
		let scram = if let Some(ref s) = v.scram {
			Some(Some(
				ScramCredential::from_verifier_string(s)
					.expect("PASSSCRAM verifier must be validated at parse time"),
			))
		} else {
			match v.pass_type {
				Some(PassType::Password(ref p)) => Some(Some(ScramCredential::generate(p))),
				Some(PassType::Hash(_)) => Some(None),
				Some(PassType::Unset) | None => None,
			}
		};

		let hash = v.pass_type.and_then(|pt| match pt {
			PassType::Unset => None,
			PassType::Hash(h) => Some(h),
			PassType::Password(p) => Some(crate::iam::hash_password(&p)),
		});

		crate::expr::statements::alter::AlterUserStatement {
			name: v.name.into(),
			base: v.base.into(),
			if_exists: v.if_exists,
			hash,
			scram,
			roles: match v.roles {
				AlterKind::Set(x) => crate::expr::statements::alter::AlterKind::Set(x),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Drop,
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			token_duration: match v.token_duration {
				AlterKind::Set(d) => crate::expr::statements::alter::AlterKind::Set(Some(d.into())),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Set(None),
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			session_duration: match v.session_duration {
				AlterKind::Set(d) => crate::expr::statements::alter::AlterKind::Set(Some(d.into())),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Set(None),
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterUserStatement> for AlterUserStatement {
	fn from(v: crate::expr::statements::alter::AlterUserStatement) -> Self {
		AlterUserStatement {
			name: v.name.into(),
			base: v.base.into(),
			if_exists: v.if_exists,
			pass_type: v.hash.map(PassType::Hash),
			// A cleared verifier (`Some(None)`) has no SQL representation; the
			// accompanying PASSHASH already conveys the password change.
			scram: v.scram.flatten().map(|c| c.to_verifier_string()),
			roles: match v.roles {
				crate::expr::statements::alter::AlterKind::Set(x) => AlterKind::Set(x),
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			token_duration: match v.token_duration {
				crate::expr::statements::alter::AlterKind::Set(Some(d)) => {
					AlterKind::Set(PublicDuration::from(d))
				}
				crate::expr::statements::alter::AlterKind::Set(None) => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			session_duration: match v.session_duration {
				crate::expr::statements::alter::AlterKind::Set(Some(d)) => {
					AlterKind::Set(PublicDuration::from(d))
				}
				crate::expr::statements::alter::AlterKind::Set(None) => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			comment: v.comment.into(),
		}
	}
}
