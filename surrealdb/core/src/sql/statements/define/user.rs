use rand::distr::{Alphanumeric, SampleString};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::catalog::ScramCredential;
use crate::fmt::{CoverStmts, EscapeKwFreeIdent, QuoteStr};
use crate::sql::{Base, Expr, Literal};
use crate::types::PublicDuration;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum PassType {
	#[default]
	Unset,
	Hash(String),
	Password(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DefineUserStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub base: Base,
	pub pass_type: PassType,
	/// Optional SCRAM-SHA-256 verifier string (`SCRAM-SHA-256$...`). Additive:
	/// it may coexist with `PASSHASH`, which is how export/import round-trips
	/// both the Argon2 hash and the SCRAM verifier. When `PASSWORD` is given and
	/// this is `None`, the verifier is derived from the plaintext.
	pub scram: Option<String>,
	pub roles: Vec<String>,
	pub token_duration: Expr,
	pub session_duration: Expr,

	pub comment: Expr,
}

impl Default for DefineUserStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			base: Base::Root,
			pass_type: PassType::Unset,
			scram: None,
			roles: vec![],
			// Tokens default to a 1-hour expiry when DURATION FOR TOKEN is
			// omitted. Sessions default to no expiry.
			token_duration: Expr::Literal(Literal::Duration(PublicDuration::from_secs(3600))),
			session_duration: Expr::Literal(Literal::None),
			comment: Expr::Literal(Literal::None),
		}
	}
}

impl ToSql for DefineUserStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "DEFINE USER");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, fmt, " IF NOT EXISTS"),
		}

		write_sql!(f, fmt, " {} ON {}", CoverStmts(&self.name), &self.base);

		match self.pass_type {
			PassType::Unset => {}
			PassType::Hash(ref x) => write_sql!(f, fmt, " PASSHASH {}", QuoteStr(x)),
			PassType::Password(ref x) => write_sql!(f, fmt, " PASSWORD {}", QuoteStr(x)),
		}

		if let Some(ref x) = self.scram {
			write_sql!(f, fmt, " PASSSCRAM {}", QuoteStr(x));
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
		CoverStmts(&self.token_duration).fmt_sql(f, fmt);
		f.push_str(", FOR SESSION ");
		CoverStmts(&self.session_duration).fmt_sql(f, fmt);
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
	}
}

#[allow(clippy::fallible_impl_from)]
impl From<DefineUserStatement> for crate::expr::statements::DefineUserStatement {
	fn from(v: DefineUserStatement) -> Self {
		// An explicit PASSSCRAM verifier takes precedence (import/round-trip);
		// otherwise derive from the plaintext password when one is provided.
		// The verifier is validated at parse time (both DEFINE USER parsers call
		// `from_verifier_string` and bail on error), so `expect` upholds that
		// invariant loudly instead of silently dropping a bad verifier — the same
		// way the Argon2 hashing treats its impossible failure.
		let scram = if let Some(ref s) = v.scram {
			Some(
				ScramCredential::from_verifier_string(s)
					.expect("PASSSCRAM verifier must be validated at parse time"),
			)
		} else if let PassType::Password(ref p) = v.pass_type {
			Some(ScramCredential::generate(p))
		} else {
			None
		};

		let hash = match v.pass_type {
			PassType::Unset => String::new(),
			PassType::Hash(x) => x,
			// TODO: Move out of AST.
			PassType::Password(p) => crate::iam::hash_password(&p),
		};

		let code = Alphanumeric.sample_string(&mut rand::rng(), 128);

		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			base: v.base.into(),
			hash,
			code,
			scram,
			roles: v.roles,
			duration: crate::expr::user::UserDuration {
				token: v.token_duration.into(),
				session: v.session_duration.into(),
			},
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::DefineUserStatement> for DefineUserStatement {
	fn from(v: crate::expr::statements::DefineUserStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			base: v.base.into(),
			pass_type: PassType::Hash(v.hash),
			scram: v.scram.as_ref().map(|c| c.to_verifier_string()),
			roles: v.roles,
			token_duration: v.duration.token.into(),
			session_duration: v.duration.session.into(),
			comment: v.comment.into(),
		}
	}
}
