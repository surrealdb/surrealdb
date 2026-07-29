use common::fmt::{EscapeKwFreeIdent, QuoteStr};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::{Base, CoverStmts, Expr, Literal};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum PassType {
	#[default]
	Unset,
	Hash(String),
	Password(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefineUserStatement {
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
			token_duration: Expr::Literal(Literal::Duration(std::time::Duration::from_secs(3600))),
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
